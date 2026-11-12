package requestfacts

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

const archiveRecordFixture = `=== REQUEST INFO ===
URL: /v1/responses
Timestamp: 2026-11-05T16:17:16.144614+03:00

=== HEADERS ===
Content-Type: application/json

=== REQUEST BODY ===
{"input":"X-Codex-Primary-Used-Percent: 1"}

=== API REQUEST 1 ===
Timestamp: 2026-11-05T16:17:16.163878+03:00
Upstream URL: https://chatgpt.com/backend-api/codex/responses
HTTP Method: POST
Auth: provider=codex, auth_id=codex-a.json, label=a@example.com, type=oauth

Headers:
Accept: text/event-stream

Body:
{}

=== API REQUEST 2 ===
Timestamp: 2026-11-05T16:17:17.000000+03:00
Upstream URL: https://api.anthropic.com/v1/messages
HTTP Method: POST
Auth: provider=claude, auth_id=claude-b.json, label=b@example.com, type=oauth

Headers:
Accept: text/event-stream

Body:
{}

=== API RESPONSE 1 ===
Timestamp: 2026-11-05T16:17:16.500000+03:00

Status: 429
Headers:
X-Codex-Primary-Used-Percent: 100
X-Codex-Primary-Window-Minutes: 300
X-Codex-Primary-Reset-At: 1793892278
X-Codex-Secondary-Used-Percent: 38
X-Codex-Secondary-Window-Minutes: 10080
X-Codex-Secondary-Reset-At: 1794396024

{"error":"usage_limit_reached"}

=== API RESPONSE 2 ===
Timestamp: 2026-11-05T16:17:18.000000+03:00

Status: 200
Headers:
Anthropic-Ratelimit-Unified-5h-Utilization: 0.74
Anthropic-Ratelimit-Unified-5h-Reset: 1793898000
Anthropic-Ratelimit-Unified-7d-Utilization: 0.56
Anthropic-Ratelimit-Unified-7d-Reset: 1794337200

data: {"type":"message_start"}

=== RESPONSE ===
X-Codex-Primary-Used-Percent: 5
`

func TestParseArchiveRecordPairsAttemptsWithResponses(t *testing.T) {
	samples, err := parseArchiveRecord(strings.NewReader(archiveRecordFixture))
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 2 {
		t.Fatalf("got %d samples, want 2: %+v", len(samples), samples)
	}
	codex, claude := samples[0], samples[1]
	if codex.AuthID != "codex-a.json" || codex.Provider != "codex" {
		t.Fatalf("codex attempt mismatched: %+v", codex)
	}
	if len(codex.Windows) != 2 || codex.Windows[0].ID != "five-hour" || codex.Windows[0].UsedPercent != 100 ||
		codex.Windows[1].ID != "weekly" || codex.Windows[1].UsedPercent != 38 || codex.Windows[1].ResetAtMs != 1794396024000 {
		t.Fatalf("codex windows: %+v", codex.Windows)
	}
	if claude.AuthID != "claude-b.json" || len(claude.Windows) != 2 ||
		claude.Windows[0].ID != "five-hour" || claude.Windows[0].UsedPercent != 74 ||
		claude.Windows[1].ID != "seven-day" || claude.Windows[1].UsedPercent != 56 {
		t.Fatalf("claude windows: %+v", claude)
	}
	want := time.Date(2026, 11, 5, 13, 17, 18, 0, time.UTC).UnixMilli()
	if claude.ObservedMs != want {
		t.Fatalf("observed %d, want %d", claude.ObservedMs, want)
	}
}

func TestParseArchiveRecordSkipsOversizedBodyLines(t *testing.T) {
	record := strings.Replace(archiveRecordFixture, `{"input":"X-Codex-Primary-Used-Percent: 1"}`,
		strings.Repeat("x", archiveMaxLine*3), 1)
	samples, err := parseArchiveRecord(strings.NewReader(record))
	if err != nil || len(samples) != 2 {
		t.Fatalf("samples=%d err=%v", len(samples), err)
	}
}

func writeArchiveFixture(t *testing.T, path string, modified time.Time) {
	t.Helper()
	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = encoder.Write([]byte(archiveRecordFixture)); err != nil {
		t.Fatal(err)
	}
	if err = encoder.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, compressed.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(path, modified, modified); err != nil {
		t.Fatal(err)
	}
}

func TestBackfillImportsArchiveOnceAndOnlyBeforeStart(t *testing.T) {
	dir := t.TempDir()
	started := time.Now()
	writeArchiveFixture(t, filepath.Join(dir, "v1-responses-a.log.zst"), started.Add(-time.Hour))
	// Finished after the store started: recorded live, so not imported.
	writeArchiveFixture(t, filepath.Join(dir, "v1-responses-b.log.zst"), started.Add(time.Hour))
	// Outside the quota span.
	writeArchiveFixture(t, filepath.Join(dir, "v1-responses-c.log.zst"), started.Add(-QuotaSpan-time.Hour))

	store := Open(dir)
	store.startedAt = started
	store.Backfill(context.Background(), dir)
	content, err := os.ReadFile(filepath.Join(dir, DirectoryName, backfillFileName))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(content), "\n"); lines != 2 {
		t.Fatalf("backfill wrote %d readings, want 2 from one record", lines)
	}
	samples, _ := store.QuotaSamples(time.Time{})
	if len(samples) != 4 {
		t.Fatalf("backfill indexed %d window readings, want 4: %+v", len(samples), samples)
	}

	// The marker makes later starts skip the archive, and a fresh store loads the file.
	writeArchiveFixture(t, filepath.Join(dir, "v1-responses-d.log.zst"), started.Add(-2*time.Hour))
	reopened := Open(dir)
	reopened.startedAt = started
	reopened.Backfill(context.Background(), dir)
	again, errRead := os.ReadFile(filepath.Join(dir, DirectoryName, backfillFileName))
	if errRead != nil || !bytes.Equal(again, content) {
		t.Fatal("completed backfill ran again")
	}
	if samples, _ = reopened.QuotaSamples(time.Time{}); len(samples) != 4 {
		t.Fatalf("reloaded %d readings, want 4", len(samples))
	}
}

func TestBackfillWithoutArchiveLeavesNoMarker(t *testing.T) {
	dir := t.TempDir()
	store := Open(dir)
	store.Backfill(context.Background(), dir)
	if _, err := os.Stat(filepath.Join(dir, DirectoryName)); !os.IsNotExist(err) {
		t.Fatalf("empty archive created facts directory: %v", err)
	}
}
