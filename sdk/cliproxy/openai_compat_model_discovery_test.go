package cliproxy

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

func TestWithDiscoveredCompatModelsKeepsConfiguredAliases(t *testing.T) {
	compat := &config.OpenAICompatibility{
		Name:    "opencode-go",
		BaseURL: "https://x/v1",
		Models: []config.OpenAICompatibilityModel{
			{Name: "deepseek-v4.1-flash", Alias: "deepseek-v4.1-flash", InputModalities: []string{"text", "image"}},
			{Name: "deepseek-v4.1-flash", Alias: "codex-auto-review"},
		},
		ModelsDiscovery: &config.OpenAICompatibilityModelsDiscovery{Enabled: true, Exclude: []string{"*-free"}},
	}
	discovered := []string{"DeepSeek-V4.1-Flash", "beam-free", "gpt-6-luna", "kimi-k3"}

	got := withDiscoveredCompatModels(compat, discovered)
	var aliases []string
	for _, model := range got {
		aliases = append(aliases, model.Alias)
	}
	want := []string{"deepseek-v4.1-flash", "codex-auto-review", "gpt-6-luna", "kimi-k3"}
	if len(aliases) != len(want) {
		t.Fatalf("aliases = %v, want %v", aliases, want)
	}
	for i := range want {
		if aliases[i] != want[i] {
			t.Fatalf("aliases = %v, want %v", aliases, want)
		}
	}
	if len(got[0].InputModalities) != 2 {
		t.Fatal("configured entry settings must be preserved")
	}
	if got[2].Name != "gpt-6-luna" {
		t.Fatalf("discovered model must be sent upstream unchanged, got %q", got[2].Name)
	}

	compat.ModelsDiscovery.Enabled = false
	if got := withDiscoveredCompatModels(compat, discovered); len(got) != 2 {
		t.Fatalf("disabled discovery must use configured models only, got %d", len(got))
	}
}

func TestBuildOpenAICompatibilityConfigModelsWithoutConfiguredModels(t *testing.T) {
	compat := &config.OpenAICompatibility{Name: "empty", BaseURL: "https://unused.invalid"}
	if models := buildOpenAICompatibilityConfigModels(compat); models != nil {
		t.Fatalf("expected no models, got %d", len(models))
	}
}
