package compatmodels

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

type fakeFetch struct {
	ids   []string
	err   error
	calls int
}

func (f *fakeFetch) fetch(context.Context, Target) ([]string, error) {
	f.calls++
	return f.ids, f.err
}

func newTestStore(f *fakeFetch, now *time.Time) *Store {
	store := NewStore(f.fetch)
	store.now = func() time.Time { return *now }
	store.jitter = func(n int64) int64 { return 0 }
	return store
}

func TestRefreshCachesUntilIntervalAndReportsChanges(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	f := &fakeFetch{ids: []string{"b", "a", "a", " "}}
	store := newTestStore(f, &now)
	target := Target{Name: "Go", BaseURL: "https://x/v1/", APIKey: "k1", Interval: 6 * time.Hour}

	if due := store.NextRefresh(target); !due.IsZero() {
		t.Fatalf("unfetched catalog should be due now, got %v", due)
	}
	changed, err := store.Refresh(context.Background(), target)
	if err != nil || !changed {
		t.Fatalf("first refresh: changed=%v err=%v", changed, err)
	}
	if got := store.Models("go", "https://x/v1"); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("models = %v", got)
	}
	if due := store.NextRefresh(target); !due.Equal(now.Add(6 * time.Hour)) {
		t.Fatalf("next refresh = %v", due)
	}
	changed, err = store.Refresh(context.Background(), target)
	if err != nil || changed {
		t.Fatalf("identical list should not report a change: changed=%v err=%v", changed, err)
	}
	rotated := target
	rotated.APIKey = "k2"
	if due := store.NextRefresh(rotated); !due.IsZero() {
		t.Fatalf("a rotated key should refresh early, got %v", due)
	}
}

func TestRefreshFailureKeepsListAndBacksOff(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	f := &fakeFetch{ids: []string{"a"}}
	store := newTestStore(f, &now)
	target := Target{Name: "go", BaseURL: "https://x", Interval: 6 * time.Hour}
	if _, err := store.Refresh(context.Background(), target); err != nil {
		t.Fatal(err)
	}

	// The catalog expires; refreshes now fail upstream.
	now = now.Add(7 * time.Hour)
	f.ids, f.err = nil, errors.New("boom")
	var delays []time.Duration
	for range 7 {
		if _, err := store.Refresh(context.Background(), target); err == nil {
			t.Fatal("expected error")
		}
		delays = append(delays, store.NextRefresh(target).Sub(now))
	}
	// Zero jitter yields half of each window: 1m, 2m, 4m ... capped at 30m.
	want := []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 30 * time.Minute, 30 * time.Minute}
	if !slices.Equal(delays, want) {
		t.Fatalf("delays = %v, want %v", delays, want)
	}
	if got := store.Models("go", "https://x"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("failure must keep previous list, got %v", got)
	}

	// An empty list is a failure, not an authoritative empty catalog.
	f.err = nil
	if _, err := store.Refresh(context.Background(), target); err == nil {
		t.Fatal("empty list should fail")
	}
	if got := store.Models("go", "https://x"); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("empty response must keep previous list, got %v", got)
	}
}

func TestFailureBeforeFirstSuccessBacksOffExponentially(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	f := &fakeFetch{err: errors.New("down")}
	store := newTestStore(f, &now)
	target := Target{Name: "go", BaseURL: "https://x", Interval: 6 * time.Hour}
	var delays []time.Duration
	for range 7 {
		_, _ = store.Refresh(context.Background(), target)
		delays = append(delays, store.NextRefresh(target).Sub(now))
	}
	want := []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 30 * time.Minute, 30 * time.Minute}
	if !slices.Equal(delays, want) {
		t.Fatalf("delays = %v, want %v", delays, want)
	}
	if status := store.Statuses([]Target{target})[0]; status.LastError != "down" || status.Failures != 7 {
		t.Fatalf("status = %+v", status)
	}
}

func TestParseModelIDs(t *testing.T) {
	cases := map[string][]string{
		`{"object":"list","data":[{"id":"m2"},{"id":"m1"}]}`: {"m1", "m2"},
		`{"models":[{"name":"n1"},{"id":"i1"}]}`:             {"i1", "n1"},
		`["s1",{"id":"o1"}]`:                                 {"o1", "s1"},
	}
	for body, want := range cases {
		got, err := ParseModelIDs([]byte(body))
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("%s: got %v err %v", body, got, err)
		}
	}
	if _, err := ParseModelIDs([]byte(`{"error":"x"}`)); err == nil {
		t.Error("object without a list should fail")
	}
}

func TestFetchModelsSendsKeyAndStaticHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" || r.Header.Get("Authorization") != "Bearer secret" ||
			r.Header.Get("User-Agent") != "ua" || r.Header.Get("X-Session") != "" {
			http.Error(w, "bad request "+r.URL.Path, http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-6-luna"}]}`))
	}))
	defer server.Close()
	ids, err := FetchModels(context.Background(), Target{
		BaseURL: server.URL + "/v1/",
		APIKey:  "secret",
		Headers: map[string]string{"User-Agent": "ua", "X-Session": "$CPA-SESSION-ID"},
	})
	if err != nil || !slices.Equal(ids, []string{"gpt-6-luna"}) {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
}

func TestTargetsSkipsDisabledAndUnweightedKeys(t *testing.T) {
	zero := 0
	cfg := &config.Config{}
	cfg.ProxyURL = "http://global"
	cfg.OpenAICompatibility = []config.OpenAICompatibility{
		{Name: "off", BaseURL: "https://a", ModelsDiscovery: &config.OpenAICompatibilityModelsDiscovery{}},
		{Name: "disabled", BaseURL: "https://b", Disabled: true, ModelsDiscovery: &config.OpenAICompatibilityModelsDiscovery{Enabled: true}},
		{
			Name:            "go",
			BaseURL:         "https://c",
			ModelsDiscovery: &config.OpenAICompatibilityModelsDiscovery{Enabled: true, RefreshInterval: "1m"},
			APIKeyEntries: []config.OpenAICompatibilityAPIKey{
				{APIKey: "skip", Weight: &zero},
				{APIKey: "use", ProxyURL: "http://key"},
			},
		},
	}
	targets := Targets(cfg)
	if len(targets) != 1 {
		t.Fatalf("targets = %+v", targets)
	}
	got := targets[0]
	if got.Name != "go" || got.APIKey != "use" || got.ProxyURL != "http://key" || got.Interval != config.MinModelsDiscoveryRefreshInterval {
		t.Fatalf("target = %+v", got)
	}
}

func TestExcluded(t *testing.T) {
	patterns := []string{"*-free", "gpt-*-preview", "exact"}
	for id, want := range map[string]bool{
		"beam-free": true, "Longcat-2.5-FREE": true, "gpt-6-preview": true, "EXACT": true,
		"gpt-6-luna": false, "free-model": false, "exactly": false,
	} {
		if got := Excluded(patterns, id); got != want {
			t.Errorf("Excluded(%q) = %v, want %v", id, got, want)
		}
	}
}
