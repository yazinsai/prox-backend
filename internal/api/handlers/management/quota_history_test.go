package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/requestfacts"
)

// quotaHistoryRecordFixture is an archived request carrying Codex and Claude
// quota headers on two upstream attempts.
const quotaHistoryRecordFixture = `=== REQUEST INFO ===
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

func TestCompactQuotaHistoryPointsKeepsRunEndpoints(t *testing.T) {
	points := []quotaHistoryWindowPoint{
		{1000, 10, 9_000_000}, {2000, 10, 9_000_500}, {3000, 10, 9_000_000}, {4000, 20, 9_000_000}, {5000, 20, 9_000_000},
	}
	got := compactQuotaHistoryPoints(points)
	if len(got) != 4 || got[0][0] != 1000 || got[1][0] != 3000 || got[2][0] != 4000 || got[3][0] != 5000 {
		t.Fatalf("got %v", got)
	}
}

func TestGetQuotaHistoryServesRequestFacts(t *testing.T) {
	store := requestfacts.Open(t.TempDir())
	now := time.Now()
	store.Append(requestfacts.Row{At: now.Add(-48 * time.Hour), AuthID: "claude-b.json", Provider: "claude",
		Quota: []requestfacts.Window{{ID: "five-hour", WindowMinutes: 300, UsedPercent: 74, ResetAtMs: 1793898000000}}})
	store.Append(requestfacts.Row{At: now.Add(-time.Hour), AuthID: "codex-a.json", Provider: "codex",
		Quota: []requestfacts.Window{{ID: "weekly", WindowMinutes: 10080, UsedPercent: 38}}})
	// Outside the requested span.
	store.Append(requestfacts.Row{At: now.Add(-20 * 24 * time.Hour), AuthID: "codex-old.json", Provider: "codex",
		Quota: []requestfacts.Window{{ID: "weekly", WindowMinutes: 10080, UsedPercent: 1}}})

	h := &Handler{cfg: &config.Config{}}
	h.SetRequestFacts(store)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v8/management/credentials/quota/history?since="+
		now.Add(-7*24*time.Hour).UTC().Format(time.RFC3339), nil)
	h.GetQuotaHistory(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
	var response quotaHistoryResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.Recording || len(response.Credentials) != 2 || response.Credentials[0].Provider != "claude" ||
		response.Credentials[1].AuthID != "codex-a.json" || response.Credentials[0].Series[0].Points[0][1] != 74 {
		t.Fatalf("response: %s", recorder.Body.String())
	}
	if response.OldestAt == nil || response.OldestAt.UnixMilli() != now.Add(-48*time.Hour).UnixMilli() {
		t.Fatalf("oldest_at %v", response.OldestAt)
	}
	if recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("history response can be cached")
	}

	bad := httptest.NewRecorder()
	badCtx, _ := gin.CreateTestContext(bad)
	badCtx.Request = httptest.NewRequest(http.MethodGet, "/x?since=yesterday", nil)
	h.GetQuotaHistory(badCtx)
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("invalid since accepted: %d", bad.Code)
	}
}

func TestGetQuotaHistoryWithoutStoreReportsNotRecording(t *testing.T) {
	h := &Handler{cfg: &config.Config{}}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v8/management/credentials/quota/history", nil)
	h.GetQuotaHistory(ctx)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"recording":false`) {
		t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
	}
}

// assertArchiveBackfill checks that the one-time quota backfill reads the
// fixture record from whatever archive layout dir holds.
func assertArchiveBackfill(t *testing.T, dir string) {
	t.Helper()
	store := requestfacts.Open(dir)
	store.Backfill(context.Background(), dir)
	samples, _ := store.QuotaSamples(time.Time{})
	if len(samples) != 4 {
		t.Fatalf("archive backfill indexed %d window readings, want 4: %+v", len(samples), samples)
	}
}
