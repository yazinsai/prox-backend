package auth

import (
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/modelnames"
)

func TestNormalizedOAuthModelResolvesThroughConfiguredAlias(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetOAuthModelAlias(map[string][]internalconfig.OAuthModelAlias{
		"antigravity": {{Name: "claude-opus-5-5-high", Alias: "claude-opus-5-5"}},
	})
	modelnames.SetEnabled(true)
	defer modelnames.SetEnabled(false)
	modelnames.RecordOriginal("antigravity", "claude-opus-5.5", "claude-opus-5-5")
	modelnames.RecordOriginal("antigravity", "gemini-3.8-flash", "gemini-3.8-flash-high")

	auth := &Auth{ID: "ag", Provider: "antigravity"}
	for requested, want := range map[string]string{
		"claude-opus-5.5":       "claude-opus-5-5-high",
		"claude-opus-5.5(high)": "claude-opus-5-5-high(high)",
		"gemini-3.8-flash":      "gemini-3.8-flash-high",
		"gemini-3-flash":        "gemini-3-flash",
	} {
		if got := m.applyOAuthModelAlias(auth, requested); got != want {
			t.Errorf("applyOAuthModelAlias(%q) = %q, want %q", requested, got, want)
		}
	}
}
