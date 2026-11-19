package cliproxy

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/modelnames"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

func TestApplyOAuthModelAliasNormalizesNames(t *testing.T) {
	cfg := &config.Config{}
	cfg.Routing.NormalizeModelNames = true
	cfg.OAuthModelAlias = map[string][]config.OAuthModelAlias{
		"antigravity": {{Name: "claude-opus-5-5-high", Alias: "claude-opus-5-5"}},
	}
	models := []*ModelInfo{
		{ID: "claude-opus-5-5-high"}, {ID: "gemini-3.8-flash-high"}, {ID: "gemini-3-flash"},
	}
	got := applyOAuthModelAliasForAuth(cfg, "antigravity", "oauth", nil, models)
	var ids []string
	for _, model := range got {
		ids = append(ids, model.ID)
	}
	want := []string{"claude-opus-5.5", "gemini-3.8-flash", "gemini-3-flash"}
	if len(ids) != len(want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("ids = %v, want %v", ids, want)
		}
	}
	if original := modelnames.Original("antigravity", "claude-opus-5.5"); original != "claude-opus-5-5" {
		t.Fatalf("recorded original = %q, want the configured alias", original)
	}
	if original := modelnames.Original("antigravity", "gemini-3.8-flash"); original != "gemini-3.8-flash-high" {
		t.Fatalf("recorded original = %q", original)
	}
}
