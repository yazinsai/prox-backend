// Package compatmodels discovers the model catalogs of OpenAI-compatible providers.
//
// A provider that enables models-discovery is listed through GET {base-url}/models.
// Catalogs change slowly, so a successful list is reused until its refresh interval
// elapses, persists across restarts through the KV store, and is never replaced by a
// failed or empty response. Failures back off exponentially instead of retrying on
// every scheduler tick.
package compatmodels

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/httpfetch"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/kvstore"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

const (
	retryBase    = 2 * time.Minute
	retryMax     = time.Hour
	fetchTimeout = 30 * time.Second
	maxBodyBytes = 8 << 20
	kvKeyPrefix  = "compat-models-discovery:v1:"
	kvTTL        = 30 * 24 * time.Hour
)

// Target identifies one provider catalog and how to request it.
type Target struct {
	Name     string
	BaseURL  string
	APIKey   string
	ProxyURL string
	Headers  map[string]string
	Interval time.Duration
	// Exclude lists wildcard patterns of discovered IDs that are never routed.
	Exclude []string
}

func catalogKey(name, baseURL string) string {
	return strings.ToLower(strings.TrimSpace(name)) + "\x00" + strings.TrimRight(strings.TrimSpace(baseURL), "/")
}

func (t Target) key() string { return catalogKey(t.Name, t.BaseURL) }

// credential fingerprints the key used for a list; a rotated key refreshes early
// because a different key may be entitled to a different catalog.
func (t Target) credential() string { return kvstore.HashKeyPart(strings.TrimSpace(t.APIKey)) }

// Fetcher lists the model IDs a target serves.
type Fetcher func(ctx context.Context, target Target) ([]string, error)

// Status describes one catalog for management views.
type Status struct {
	Name        string    `json:"name"`
	Models      []string  `json:"models"`
	FetchedAt   time.Time `json:"fetched-at,omitzero"`
	NextRefresh time.Time `json:"next-refresh,omitzero"`
	LastError   string    `json:"last-error,omitempty"`
	Failures    int       `json:"failures,omitempty"`
}

type entry struct {
	models     []string
	fetchedAt  time.Time
	credential string
	failures   int
	nextRetry  time.Time
	lastError  string
	seeded     bool
}

type persisted struct {
	Models     []string  `json:"models"`
	FetchedAt  time.Time `json:"fetched_at"`
	Credential string    `json:"credential"`
}

// Store caches discovered catalogs. It is safe for concurrent use.
type Store struct {
	mu      sync.Mutex
	entries map[string]*entry
	group   singleflight.Group
	fetch   Fetcher
	now     func() time.Time
	jitter  func(int64) int64
	persist bool
}

// NewStore returns a store using fetch. Persistence is enabled only for the
// process-wide default store so tests never touch the shared KV store.
func NewStore(fetch Fetcher) *Store {
	return &Store{entries: make(map[string]*entry), fetch: fetch, now: time.Now, jitter: rand.Int64N}
}

var defaultStore = func() *Store {
	store := NewStore(FetchModels)
	store.persist = true
	return store
}()

// Default returns the process-wide store.
func Default() *Store { return defaultStore }

// entryLocked returns the entry for key, seeding it from the KV store once.
func (s *Store) entryLocked(key string) *entry {
	current := s.entries[key]
	if current == nil {
		current = &entry{}
		s.entries[key] = current
	}
	if current.seeded || !s.persist {
		current.seeded = true
		return current
	}
	var saved persisted
	kvMode, found := kvstore.KVGetJSONBestEffort(context.Background(), kvKeyPrefix+kvstore.HashKeyPart(key), &saved)
	if !kvMode {
		// The KV store may not be configured yet during startup; try again later.
		return current
	}
	current.seeded = true
	if found && len(saved.Models) > 0 && current.models == nil {
		current.models = normalizeIDs(saved.Models)
		current.fetchedAt = saved.FetchedAt
		current.credential = saved.Credential
	}
	return current
}

// Models returns the last successful catalog for a provider, or nil.
func (s *Store) Models(name, baseURL string) []string {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.entryLocked(catalogKey(name, baseURL)).models)
}

// NextRefresh returns when target should next be fetched. A catalog that is
// missing or was listed with another key is due immediately unless backing off.
func (s *Store) NextRefresh(target Target) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextRefreshLocked(s.entryLocked(target.key()), target)
}

func (s *Store) nextRefreshLocked(current *entry, target Target) time.Time {
	due := time.Time{}
	if current.models != nil && current.credential == target.credential() {
		due = current.fetchedAt.Add(target.Interval)
	}
	if current.nextRetry.After(due) {
		due = current.nextRetry
	}
	return due
}

// Refresh fetches target now. It reports whether the catalog changed; on failure
// the previous catalog is kept and the next attempt is delayed.
func (s *Store) Refresh(ctx context.Context, target Target) (bool, error) {
	key := target.key()
	result, errDo, _ := s.group.Do(key, func() (any, error) {
		fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
		defer cancel()
		ids, errFetch := s.fetch(fetchCtx, target)
		ids = normalizeIDs(ids)
		if errFetch == nil && len(ids) == 0 {
			errFetch = fmt.Errorf("empty model list")
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		current := s.entryLocked(key)
		now := s.now()
		if errFetch != nil {
			if ctx.Err() != nil {
				// Shutdown or reconfiguration, not an upstream failure.
				return false, errFetch
			}
			current.failures = min(current.failures+1, 8)
			window := min(retryBase<<(current.failures-1), retryMax, max(target.Interval, retryBase))
			half := window / 2
			current.nextRetry = now.Add(half + time.Duration(s.jitter(int64(half)+1)))
			current.lastError = errFetch.Error()
			return false, errFetch
		}
		changed := !slices.Equal(current.models, ids)
		current.models = ids
		current.fetchedAt = now
		current.credential = target.credential()
		current.failures = 0
		current.nextRetry = time.Time{}
		current.lastError = ""
		if s.persist {
			kvstore.KVSetJSONBestEffort(context.Background(), kvKeyPrefix+kvstore.HashKeyPart(key),
				persisted{Models: ids, FetchedAt: now, Credential: current.credential}, kvTTL)
		}
		return changed, nil
	})
	changed, _ := result.(bool)
	return changed, errDo
}

// Statuses reports the catalogs of the given targets in order.
func (s *Store) Statuses(targets []Target) []Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Status, 0, len(targets))
	for _, target := range targets {
		current := s.entryLocked(target.key())
		out = append(out, Status{
			Name:        strings.TrimSpace(target.Name),
			Models:      FilterExcluded(current.models, target.Exclude),
			FetchedAt:   current.fetchedAt,
			NextRefresh: s.nextRefreshLocked(current, target),
			LastError:   current.lastError,
			Failures:    current.failures,
		})
	}
	return out
}

// Retain drops in-memory state for catalogs no longer configured. Persisted
// catalogs expire on their own, so re-enabling a provider can reuse them.
func (s *Store) Retain(targets []Target) {
	keep := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		keep[target.key()] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.entries {
		if _, ok := keep[key]; !ok {
			delete(s.entries, key)
		}
	}
}

func normalizeIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// FetchModels lists GET {base-url}/models with the provider's key, proxy, and
// static headers. Headers with runtime placeholders ("$CPA-...") are skipped.
func FetchModels(ctx context.Context, target Target) ([]string, error) {
	client := &http.Client{Timeout: fetchTimeout}
	transport, _, errProxy := proxyutil.BuildHTTPTransport(strings.TrimSpace(target.ProxyURL))
	if errProxy != nil {
		return nil, fmt.Errorf("proxy: %w", errProxy)
	}
	if transport != nil {
		client.Transport = transport
	}
	headers := map[string]string{"Accept": "application/json"}
	for name, value := range target.Headers {
		if strings.Contains(value, "$") {
			continue
		}
		headers[name] = value
	}
	if key := strings.TrimSpace(target.APIKey); key != "" {
		hasAuth := false
		for name := range headers {
			hasAuth = hasAuth || strings.EqualFold(name, "Authorization")
		}
		if !hasAuth {
			headers["Authorization"] = "Bearer " + key
		}
	}
	url := strings.TrimRight(strings.TrimSpace(target.BaseURL), "/") + "/models"
	body, errGet := httpfetch.GetBytes(ctx, client, url, headers, maxBodyBytes)
	if errGet != nil {
		return nil, errGet
	}
	return ParseModelIDs(body)
}

// ParseModelIDs reads an OpenAI-style list ({"data":[{"id":...}]}), a
// {"models":[...]} object, or a bare array of objects or strings.
func ParseModelIDs(body []byte) ([]string, error) {
	var root any
	if errUnmarshal := json.Unmarshal(body, &root); errUnmarshal != nil {
		return nil, fmt.Errorf("decode model list: %w", errUnmarshal)
	}
	var items []any
	switch value := root.(type) {
	case []any:
		items = value
	case map[string]any:
		if data, ok := value["data"].([]any); ok {
			items = data
		} else if models, ok := value["models"].([]any); ok {
			items = models
		} else {
			return nil, fmt.Errorf("model list has no data array")
		}
	default:
		return nil, fmt.Errorf("unexpected model list shape")
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		switch value := item.(type) {
		case string:
			ids = append(ids, value)
		case map[string]any:
			if id, ok := value["id"].(string); ok && strings.TrimSpace(id) != "" {
				ids = append(ids, id)
			} else if name, ok := value["name"].(string); ok {
				ids = append(ids, name)
			}
		}
	}
	return normalizeIDs(ids), nil
}

// LogRefresh records a refresh outcome without leaking credentials.
func LogRefresh(name string, changed bool, count int, err error) {
	switch {
	case err != nil:
		log.Warnf("models-discovery: %s refresh failed, keeping previous list: %v", name, err)
	case changed:
		log.Infof("models-discovery: %s now lists %d models", name, count)
	default:
		log.Debugf("models-discovery: %s list unchanged (%d models)", name, count)
	}
}

// Targets lists the enabled catalogs of cfg. The first usable key lists the
// catalog; keys of one provider are expected to share entitlements.
func Targets(cfg *config.Config) []Target {
	if cfg == nil {
		return nil
	}
	targets := make([]Target, 0)
	for i := range cfg.OpenAICompatibility {
		compat := &cfg.OpenAICompatibility[i]
		if !compat.DiscoveryEnabled() {
			continue
		}
		target := Target{
			Name:     compat.Name,
			BaseURL:  compat.BaseURL,
			ProxyURL: strings.TrimSpace(cfg.ProxyURL),
			Headers:  compat.Headers,
			Interval: compat.ModelsDiscovery.Interval(),
			Exclude:  compat.ModelsDiscovery.Exclude,
		}
		for _, key := range compat.APIKeyEntries {
			if strings.TrimSpace(key.APIKey) == "" || (key.Weight != nil && *key.Weight <= 0) {
				continue
			}
			target.APIKey = key.APIKey
			if proxyURL := strings.TrimSpace(key.ProxyURL); proxyURL != "" {
				target.ProxyURL = proxyURL
			}
			break
		}
		targets = append(targets, target)
	}
	return targets
}

// FilterExcluded returns ids without those matching a pattern; "*" matches any run
// of characters and comparison ignores case.
func FilterExcluded(ids []string, patterns []string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if !Excluded(patterns, id) {
			out = append(out, id)
		}
	}
	return out
}

// Excluded reports whether id matches any pattern.
func Excluded(patterns []string, id string) bool {
	id = strings.ToLower(id)
	for _, pattern := range patterns {
		if pattern = strings.ToLower(strings.TrimSpace(pattern)); pattern != "" && wildcardMatch(pattern, id) {
			return true
		}
	}
	return false
}

func wildcardMatch(pattern, value string) bool {
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == value
	}
	if !strings.HasPrefix(value, parts[0]) {
		return false
	}
	value = value[len(parts[0]):]
	last := parts[len(parts)-1]
	for _, part := range parts[1 : len(parts)-1] {
		index := strings.Index(value, part)
		if index < 0 {
			return false
		}
		value = value[index+len(part):]
	}
	return strings.HasSuffix(value, last)
}
