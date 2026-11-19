package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/compatmodels"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestNormalizedDiscoveredModelResolvesToUpstreamID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":"gemini-3-7-flash"},{"id":"qwen4-max"}]}`))
	}))
	defer server.Close()

	cfg := &internalconfig.Config{OpenAICompatibility: []internalconfig.OpenAICompatibility{{
		Name:            "normalize-fixture",
		BaseURL:         server.URL,
		APIKeyEntries:   []internalconfig.OpenAICompatibilityAPIKey{{APIKey: "fixture-key"}},
		ModelsDiscovery: &internalconfig.OpenAICompatibilityModelsDiscovery{Enabled: true, NormalizeNames: true},
	}}}
	targets := compatmodels.Targets(cfg)
	if _, err := compatmodels.Default().Refresh(context.Background(), targets[0]); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	defer compatmodels.Default().Retain(nil)

	auth := &Auth{
		ID:         "compat-fixture",
		Provider:   "normalize-fixture",
		Attributes: map[string]string{"api_key": "fixture-key", "compat_name": "normalize-fixture", "provider_key": "normalize-fixture"},
	}
	for requested, want := range map[string]string{"gemini-3.7-flash": "gemini-3-7-flash", "qwen-4-max": "qwen4-max"} {
		if got := resolveUpstreamModelForOpenAICompatAPIKey(cfg, auth, requested); got != want {
			t.Errorf("resolve(%q) = %q, want %q", requested, got, want)
		}
	}

	cfg.OpenAICompatibility[0].ModelsDiscovery.NormalizeNames = false
	if got := resolveUpstreamModelForOpenAICompatAPIKey(cfg, auth, "gemini-3-7-flash"); got != "" {
		t.Errorf("without normalize-names a raw discovered ID needs no mapping, got %q", got)
	}
}
