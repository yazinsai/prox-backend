package requestfacts

import (
	"math"
	"net/http"
	"strconv"
	"strings"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// Window is one account-level quota window reading.
type Window struct {
	ID            string  `json:"id"`
	WindowMinutes int     `json:"window_minutes"`
	UsedPercent   float64 `json:"used_percent"`
	ResetAtMs     int64   `json:"reset_at_ms,omitempty"`
}

// QuotaWindows maps provider rate-limit response headers to the account-level
// windows the management UI draws. Window IDs match the IDs the UI already uses
// for live quota (Claude five-hour/seven-day, Codex five-hour/weekly).
// Model-scoped and additional limits are not part of the account pool and are
// left out.
func QuotaWindows(provider string, headers http.Header) []Window {
	provider = strings.ToLower(strings.TrimSpace(provider))
	signals := coreauth.CollectQuotaSignals(provider, headers)
	if len(signals) == 0 {
		return nil
	}
	lookup := make(map[string]string, len(signals))
	for key, value := range signals {
		lookup[strings.ToLower(key)] = value
	}
	number := func(key string) (float64, bool) {
		value, ok := lookup[key]
		if !ok {
			return 0, false
		}
		parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return 0, false
		}
		return parsed, true
	}

	var windows []Window
	switch provider {
	case "claude":
		for _, claim := range []struct {
			key, id string
			minutes int
		}{{"5h", "five-hour", 300}, {"7d", "seven-day", 7 * 24 * 60}} {
			utilization, ok := number("anthropic-ratelimit-unified-" + claim.key + "-utilization")
			if !ok {
				continue
			}
			window := Window{ID: claim.id, WindowMinutes: claim.minutes, UsedPercent: clampPercent(utilization * 100)}
			if reset, okReset := number("anthropic-ratelimit-unified-" + claim.key + "-reset"); okReset {
				window.ResetAtMs = int64(reset * 1000)
			}
			windows = append(windows, window)
		}
	case "codex":
		for _, slot := range []string{"primary", "secondary"} {
			used, ok := number("x-codex-" + slot + "-used-percent")
			if !ok {
				continue
			}
			minutes, okMinutes := number("x-codex-" + slot + "-window-minutes")
			if !okMinutes || minutes <= 0 {
				continue
			}
			window := Window{WindowMinutes: int(minutes), UsedPercent: clampPercent(used)}
			switch window.WindowMinutes {
			case 300:
				window.ID = "five-hour"
			case 7 * 24 * 60:
				window.ID = "weekly"
			default:
				window.ID = slot
			}
			if reset, okReset := number("x-codex-" + slot + "-reset-at"); okReset {
				window.ResetAtMs = int64(reset * 1000)
			}
			windows = append(windows, window)
		}
	}
	return windows
}

func clampPercent(value float64) float64 {
	return math.Round(math.Min(100, math.Max(0, value))*100) / 100
}
