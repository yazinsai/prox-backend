package compatmodels

import (
	"github.com/router-for-me/CLIProxyAPI/v8/internal/modelnames"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestMergeDiscoveredNormalizesNames(t *testing.T) {
	compat := &config.OpenAICompatibility{
		Name:    "venice",
		BaseURL: "https://x/v1",
		Models: []config.OpenAICompatibilityModel{
			{Name: "gemini-3-8-flash-custom", Alias: "gemini-3.8-flash"},
		},
		ModelsDiscovery: &config.OpenAICompatibilityModelsDiscovery{
			Enabled: true, Exclude: []string{"e2ee-*"},
		},
	}
	discovered := []string{"e2ee-glm-5-3-p", "gemini-3-7-flash", "gemini-3-8-flash", "qwen-3-8-max", "qwen3-8-max"}
	modelnames.SetEnabled(true)
	defer modelnames.SetEnabled(false)
	got := MergeDiscovered(compat, discovered)
	want := []config.OpenAICompatibilityModel{
		{Name: "gemini-3-8-flash-custom", Alias: "gemini-3.8-flash"},
		{Name: "gemini-3-7-flash", Alias: "gemini-3.7-flash"},
		{Name: "qwen-3-8-max", Alias: "qwen-3.8-max"},
	}
	if len(got) != len(want) {
		t.Fatalf("MergeDiscovered = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i].Name != want[i].Name || got[i].Alias != want[i].Alias {
			t.Fatalf("MergeDiscovered = %+v, want %+v", got, want)
		}
	}
	if aliases := DiscoveredAliases(compat, discovered); len(aliases) != 2 || aliases[0] != "gemini-3.7-flash" {
		t.Fatalf("DiscoveredAliases = %v", aliases)
	}

	modelnames.SetEnabled(false)
	if got := MergeDiscovered(compat, discovered); got[1].Alias != "gemini-3-7-flash" {
		t.Fatalf("without normalization discovered IDs keep their name, got %q", got[1].Alias)
	}
}
