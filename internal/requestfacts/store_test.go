package requestfacts

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

func fixedStore(t *testing.T, now time.Time) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	store := Open(dir)
	store.now = func() time.Time { return now }
	return store, filepath.Join(dir, DirectoryName)
}

func TestHandleUsageWritesSanitizedRowWithQuota(t *testing.T) {
	now := time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)
	store, dir := fixedStore(t, now)
	headers := http.Header{}
	headers.Set("Anthropic-Ratelimit-Unified-5h-Utilization", "0.42")
	headers.Set("Anthropic-Ratelimit-Unified-5h-Reset", "1793898000")
	store.HandleUsage(context.Background(), usage.Record{
		RequestID: "req-1", TraceID: "trace-1", SessionID: "session-1",
		Provider: "Claude", AuthID: "claude-a.json", AuthIndex: "3", AuthType: "oauth",
		Model: "claude-opus", Alias: "opus", APIKey: "sk-client-secret", AccessTokenSHA256: "token-fingerprint",
		Source: "person@example.com", RequestedAt: now.Add(-2 * time.Second), Latency: 2 * time.Second, TTFT: 300 * time.Millisecond,
		Failed: true, Fail: usage.Failure{StatusCode: 429, Body: "upstream error text"},
		Detail:          usage.Detail{InputTokens: 1000, OutputTokens: 50, CacheReadTokens: 800, TotalTokens: 1050},
		ResponseHeaders: headers,
	})

	content, err := os.ReadFile(filepath.Join(dir, "facts-2026-11-12.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"sk-client-secret", "token-fingerprint", "person@example.com", "upstream error text"} {
		if strings.Contains(string(content), secret) {
			t.Fatalf("row leaked %q: %s", secret, content)
		}
	}
	row := ojson.ParseBytes(content)
	checks := map[string]string{
		"provider": "claude", "auth_id": "claude-a.json", "requested_model": "opus", "reason": "rate_limited",
		"at": "2026-11-12T09:00:00Z", "session_id": "session-1",
	}
	for key, want := range checks {
		if got := row.Get(key).String(); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	if row.Get("status_code").Int() != 429 || row.Get("cache_read_tokens").Int() != 800 || row.Get("ttft_ms").Int() != 300 {
		t.Fatalf("numeric fields: %s", content)
	}
	if row.Get("quota.0.id").String() != "five-hour" || row.Get("quota.0.used_percent").Float() != 42 {
		t.Fatalf("quota: %s", content)
	}
	samples, oldest := store.QuotaSamples(now.Add(-time.Hour))
	if len(samples) != 1 || samples[0].Windows[0].UsedPercent != 42 || oldest != now.UnixMilli() {
		t.Fatalf("samples %+v oldest %d", samples, oldest)
	}
}

func TestQuotaReadingsSurviveRestartAndCompact(t *testing.T) {
	now := time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)
	store, dir := fixedStore(t, now)
	reading := func(at time.Time, used float64) Row {
		return Row{At: at, AuthID: "codex-a.json", Provider: "codex",
			Quota: []Window{{ID: "weekly", WindowMinutes: 10080, UsedPercent: used, ResetAtMs: 1794396024000}}}
	}
	for i := 0; i < 10; i++ {
		store.Append(reading(now.Add(time.Duration(i-20)*time.Minute), 10))
	}
	store.Append(reading(now.Add(-5*time.Minute), 12))
	samples, _ := store.QuotaSamples(time.Time{})
	if len(samples) != 3 {
		t.Fatalf("unchanged run kept %d points, want its endpoints and the change", len(samples))
	}

	reopened := Open(filepath.Dir(dir))
	reopened.now = store.now
	samples, _ = reopened.QuotaSamples(time.Time{})
	if len(samples) != 3 {
		t.Fatalf("restart loaded %d points, want 3", len(samples))
	}
	// Readings appended before the first query merge with persisted ones.
	reopened.Append(reading(now, 15))
	if samples, _ = reopened.QuotaSamples(time.Time{}); len(samples) != 4 {
		t.Fatalf("after append %d points, want 4", len(samples))
	}
}

func TestRotationPrunesExpiredFilesAndRepairsPartialLine(t *testing.T) {
	now := time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)
	store, dir := fixedStore(t, now)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	expired := filepath.Join(dir, "facts-"+now.Add(-Retention-48*time.Hour).Format(dayLayout)+".jsonl")
	kept := filepath.Join(dir, "facts-"+now.Add(-Retention+48*time.Hour).Format(dayLayout)+".jsonl")
	today := filepath.Join(dir, "facts-2026-11-12.jsonl")
	for path, content := range map[string]string{expired: "{}\n", kept: "{}\n", today: `{"schema_version":1,"trunc`} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store.Append(Row{At: now, AuthID: "a", Provider: "codex", Quota: []Window{{ID: "weekly", WindowMinutes: 10080, UsedPercent: 5}}})
	if _, err := os.Stat(expired); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("expired fact file survived rotation")
	}
	if _, err := os.Stat(kept); err != nil {
		t.Fatal("retained fact file was pruned")
	}
	content, err := os.ReadFile(today)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 2 || !ojson.Valid(lines[1]) {
		t.Fatalf("new row joined a partial line: %q", content)
	}
	reopened := Open(filepath.Dir(dir))
	reopened.now = store.now
	if samples, _ := reopened.QuotaSamples(time.Time{}); len(samples) != 1 {
		t.Fatalf("partial line broke loading: %d samples", len(samples))
	}
}

func TestQuotaWindowsIgnoresProvidersWithoutAccountWindows(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Codex-Primary-Used-Percent", "20")
	headers.Set("X-Codex-Primary-Window-Minutes", "300")
	if windows := QuotaWindows("gemini", headers); len(windows) != 0 {
		t.Fatalf("gemini windows: %+v", windows)
	}
	if windows := QuotaWindows("codex", headers); len(windows) != 1 || windows[0].ID != "five-hour" {
		t.Fatalf("codex windows: %+v", windows)
	}
}
