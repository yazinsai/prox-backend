package requestfacts

import (
	"strings"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/dashboard"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

// SchemaVersion is written into every row so readers can detect format changes.
const SchemaVersion = 1

// Row is the metadata of one upstream attempt. It deliberately omits request
// and response bodies, client API keys, access-token fingerprints, upstream
// error text, and account labels; the credential is identified only by its
// auth ID and index.
type Row struct {
	At              time.Time
	RequestedAt     time.Time
	RequestID       string
	TraceID         string
	SessionID       string
	ParentSessionID string

	Provider     string
	ExecutorType string
	AuthID       string
	AuthIndex    string
	AuthType     string

	Model           string
	RequestedModel  string
	ResponseModel   string
	ReasoningEffort string
	ServiceTier     string
	Stream          bool

	LatencyMs   int64
	TTFTMs      int64
	Failed      bool
	StatusCode  int
	Reason      string
	FailureCode string

	InputTokens         int64
	OutputTokens        int64
	ReasoningTokens     int64
	CachedTokens        int64
	CacheReadTokens     int64
	CacheCreationTokens int64
	TotalTokens         int64

	Quota []Window
}

// FromRecord converts a usage record into a row.
func FromRecord(record usage.Record) Row {
	requestedAt := record.RequestedAt.UTC()
	at := requestedAt.Add(record.Latency)
	if requestedAt.IsZero() {
		at = time.Now().UTC()
	}
	status := 200
	reason := ""
	if record.Failed {
		status = record.Fail.StatusCode
		reason = dashboard.FailureReason(status, record.Fail.Code)
	}
	serviceTier := record.EffectiveServiceTier
	if serviceTier == "" {
		serviceTier = record.ServiceTier
	}
	return Row{
		At:                  at,
		RequestedAt:         requestedAt,
		RequestID:           record.RequestID,
		TraceID:             record.TraceID,
		SessionID:           record.SessionID,
		ParentSessionID:     record.ParentSessionID,
		Provider:            strings.ToLower(strings.TrimSpace(record.Provider)),
		ExecutorType:        record.ExecutorType,
		AuthID:              record.AuthID,
		AuthIndex:           record.AuthIndex,
		AuthType:            record.AuthType,
		Model:               record.Model,
		RequestedModel:      record.Alias,
		ResponseModel:       record.ResponseModel,
		ReasoningEffort:     record.ReasoningEffort,
		ServiceTier:         serviceTier,
		Stream:              record.Stream,
		LatencyMs:           record.Latency.Milliseconds(),
		TTFTMs:              record.TTFT.Milliseconds(),
		Failed:              record.Failed,
		StatusCode:          status,
		Reason:              reason,
		FailureCode:         record.Fail.Code,
		InputTokens:         record.Detail.InputTokens,
		OutputTokens:        record.Detail.OutputTokens,
		ReasoningTokens:     record.Detail.ReasoningTokens,
		CachedTokens:        record.Detail.CachedTokens,
		CacheReadTokens:     record.Detail.CacheReadTokens,
		CacheCreationTokens: record.Detail.CacheCreationTokens,
		TotalTokens:         record.Detail.TotalTokens,
		Quota:               QuotaWindows(record.Provider, record.ResponseHeaders),
	}
}

// encode renders the row as one flat JSON line, so tools such as DuckDB can
// query the day files directly.
func (row Row) encode() ([]byte, error) {
	doc := ojson.NewDocString("{}")
	fields := []struct {
		key   string
		value any
	}{
		{"schema_version", SchemaVersion},
		{"at", row.At.UTC().Format(time.RFC3339Nano)},
		{"requested_at", row.RequestedAt.UTC().Format(time.RFC3339Nano)},
		{"request_id", row.RequestID},
		{"trace_id", row.TraceID},
		{"session_id", row.SessionID},
		{"parent_session_id", row.ParentSessionID},
		{"provider", row.Provider},
		{"executor_type", row.ExecutorType},
		{"auth_id", row.AuthID},
		{"auth_index", row.AuthIndex},
		{"auth_type", row.AuthType},
		{"model", row.Model},
		{"requested_model", row.RequestedModel},
		{"response_model", row.ResponseModel},
		{"reasoning_effort", row.ReasoningEffort},
		{"service_tier", row.ServiceTier},
		{"stream", row.Stream},
		{"latency_ms", row.LatencyMs},
		{"ttft_ms", row.TTFTMs},
		{"failed", row.Failed},
		{"status_code", row.StatusCode},
		{"reason", row.Reason},
		{"failure_code", row.FailureCode},
		{"input_tokens", row.InputTokens},
		{"output_tokens", row.OutputTokens},
		{"reasoning_tokens", row.ReasoningTokens},
		{"cached_tokens", row.CachedTokens},
		{"cache_read_tokens", row.CacheReadTokens},
		{"cache_creation_tokens", row.CacheCreationTokens},
		{"total_tokens", row.TotalTokens},
	}
	for _, field := range fields {
		if err := doc.Set(field.key, field.value); err != nil {
			return nil, err
		}
	}
	if len(row.Quota) > 0 {
		if err := doc.Set("quota", row.Quota); err != nil {
			return nil, err
		}
	}
	return append(doc.Bytes(), '\n'), nil
}

// decodeQuota reads only the quota fields of an encoded row.
func decodeQuota(line []byte) (authID, provider string, atMs int64, windows []Window, ok bool) {
	value := ojson.ParseBytes(line)
	if !value.IsObject() || value.Get("schema_version").Int() != SchemaVersion {
		return "", "", 0, nil, false
	}
	authID = value.Get("auth_id").String()
	provider = value.Get("provider").String()
	at, err := time.Parse(time.RFC3339Nano, value.Get("at").String())
	if authID == "" || provider == "" || err != nil {
		return "", "", 0, nil, false
	}
	for _, item := range value.Get("quota").Array() {
		window := Window{
			ID:            item.Get("id").String(),
			WindowMinutes: int(item.Get("window_minutes").Int()),
			UsedPercent:   item.Get("used_percent").Float(),
			ResetAtMs:     item.Get("reset_at_ms").Int(),
		}
		if window.ID != "" && window.WindowMinutes > 0 {
			windows = append(windows, window)
		}
	}
	return authID, provider, at.UnixMilli(), windows, len(windows) > 0
}
