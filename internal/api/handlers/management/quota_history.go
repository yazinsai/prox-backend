package management

import (
	"errors"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/requestfacts"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

const (
	quotaHistoryDefaultSpan = 14 * 24 * time.Hour
	quotaHistoryMaxSpan     = requestfacts.QuotaSpan
	// Codex reports reset instants with one-second jitter between responses.
	quotaHistoryResetTolerance = 2 * time.Minute
)

type quotaHistorySeries struct {
	ID            string `json:"id"`
	WindowMinutes int    `json:"window_minutes"`
	// Points are [observed_at_ms, used_percent, reset_at_ms] change points.
	Points [][3]float64 `json:"points"`
}

type quotaHistoryCredential struct {
	AuthID    string               `json:"auth_id"`
	AuthIndex string               `json:"auth_index,omitempty"`
	Name      string               `json:"name,omitempty"`
	Provider  string               `json:"provider"`
	Series    []quotaHistorySeries `json:"series"`
}

type quotaHistoryResponse struct {
	Recording   bool                     `json:"recording"`
	Since       time.Time                `json:"since"`
	Until       time.Time                `json:"until"`
	OldestAt    *time.Time               `json:"oldest_at"`
	Credentials []quotaHistoryCredential `json:"credentials"`
}

// GetQuotaHistory returns per-credential quota usage over time.
//
// Readings come from the request facts store, which records the quota headers
// of every upstream response as it arrives (Claude and Codex carry them), plus
// a one-time import from the request archive for earlier traffic. The live
// credential snapshot is replaced on every response, so it cannot serve history.
func (h *Handler) GetQuotaHistory(c *gin.Context) {
	if h == nil || h.cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	now := time.Now()
	since, errSince := parseQuotaHistorySince(c.Query("since"), now)
	if errSince != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errSince.Error()})
		return
	}

	h.mu.Lock()
	facts := h.requestFacts
	manager := h.authManager
	h.mu.Unlock()
	samples, oldest := facts.QuotaSamples(since)

	auths := make(map[string]*coreauth.Auth)
	if manager != nil {
		for _, auth := range manager.List() {
			if auth != nil {
				auths[auth.ID] = auth
			}
		}
	}

	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, buildQuotaHistoryResponse(samples, oldest, auths, facts.Recording(), since, now))
}

// SetRequestFacts installs the store that serves quota history. A nil store
// reports that history is not being recorded.
func (h *Handler) SetRequestFacts(store *requestfacts.Store) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.requestFacts = store
	h.mu.Unlock()
}

func parseQuotaHistorySince(raw string, now time.Time) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return now.Add(-quotaHistoryDefaultSpan), nil
	}
	since, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, errors.New("since must be an RFC 3339 timestamp")
	}
	if earliest := now.Add(-quotaHistoryMaxSpan); since.Before(earliest) {
		since = earliest
	}
	return since, nil
}

func buildQuotaHistoryResponse(samples []requestfacts.QuotaSample, oldest int64, auths map[string]*coreauth.Auth, recording bool, since, now time.Time) quotaHistoryResponse {
	response := quotaHistoryResponse{Recording: recording, Since: since.UTC(), Until: now.UTC(), Credentials: []quotaHistoryCredential{}}
	sinceMs := since.UnixMilli()

	type seriesKey struct{ authID, provider, window string }
	grouped := make(map[seriesKey][]quotaHistoryWindowPoint)
	minutes := make(map[seriesKey]int)
	for _, sample := range samples {
		if sample.ObservedMs < sinceMs {
			continue
		}
		for _, window := range sample.Windows {
			key := seriesKey{sample.AuthID, sample.Provider, window.ID}
			grouped[key] = append(grouped[key], quotaHistoryWindowPoint{sample.ObservedMs, window.UsedPercent, window.ResetAtMs})
			minutes[key] = window.WindowMinutes
		}
	}
	if oldest != 0 {
		at := time.UnixMilli(oldest).UTC()
		response.OldestAt = &at
	}

	credentials := make(map[string]*quotaHistoryCredential)
	for key, points := range grouped {
		credential := credentials[key.authID+"\x00"+key.provider]
		if credential == nil {
			credential = &quotaHistoryCredential{AuthID: key.authID, Provider: key.provider}
			if auth := auths[key.authID]; auth != nil {
				auth.EnsureIndex()
				credential.AuthIndex = auth.Index
				credential.Name = strings.TrimSpace(auth.FileName)
				if credential.Name == "" {
					credential.Name = auth.ID
				}
			}
			credentials[key.authID+"\x00"+key.provider] = credential
		}
		credential.Series = append(credential.Series, quotaHistorySeries{
			ID:            key.window,
			WindowMinutes: minutes[key],
			Points:        compactQuotaHistoryPoints(points),
		})
	}
	for _, credential := range credentials {
		sort.Slice(credential.Series, func(i, j int) bool {
			if credential.Series[i].WindowMinutes != credential.Series[j].WindowMinutes {
				return credential.Series[i].WindowMinutes < credential.Series[j].WindowMinutes
			}
			return credential.Series[i].ID < credential.Series[j].ID
		})
		response.Credentials = append(response.Credentials, *credential)
	}
	sort.Slice(response.Credentials, func(i, j int) bool {
		a, b := response.Credentials[i], response.Credentials[j]
		if a.Provider != b.Provider {
			return a.Provider < b.Provider
		}
		return a.AuthID < b.AuthID
	})
	return response
}

type quotaHistoryWindowPoint struct {
	observedMs int64
	used       float64
	resetAtMs  int64
}

// compactQuotaHistoryPoints keeps only the endpoints of runs with an unchanged
// reading. A busy credential produces a sample per request; consecutive
// identical readings add bytes without changing the drawn line.
func compactQuotaHistoryPoints(points []quotaHistoryWindowPoint) [][3]float64 {
	sort.Slice(points, func(i, j int) bool { return points[i].observedMs < points[j].observedMs })
	same := func(a, b quotaHistoryWindowPoint) bool {
		return math.Abs(a.used-b.used) < 0.05 &&
			time.Duration(absInt64(a.resetAtMs-b.resetAtMs))*time.Millisecond <= quotaHistoryResetTolerance
	}
	out := make([][3]float64, 0, len(points))
	for i, point := range points {
		if i > 0 && i < len(points)-1 && same(points[i-1], point) && same(point, points[i+1]) {
			continue
		}
		out = append(out, [3]float64{float64(point.observedMs), point.used, float64(point.resetAtMs)})
	}
	return out
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
