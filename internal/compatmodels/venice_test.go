package compatmodels

import (
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/modelnames"
)

func TestNormalizeVenice(t *testing.T) {
	for id, want := range map[string]string{
		"openai-gpt-61-sol":                         "gpt-6.1-sol",
		"openai-gpt-72-sol-high":                    "gpt-7.2-sol",
		"openai-gpt-62-astra-pro":                   "gpt-6.2-astra-pro",
		"openai-gpt-6.1-sol":                        "gpt-6.1-sol",
		"openai-gpt-6-luna":                         "gpt-6-luna",
		"openai-gpt-oss-120b":                       "gpt-oss-120b",
		"openai-gpt-4o-2024-11-20":                  "gpt-4o-2024-11-20",
		"openai-gpt-100-sol":                        "gpt-100-sol",
		"minimax-m27":                               "minimax-m2.7",
		"minimax-m38-fast":                          "minimax-m3.8-fast",
		"minimax-m3-1-flash":                        "minimax-m3.1-flash",
		"z-ai-glm-5-3":                              "glm-5.3",
		"zai-org-glm-4.7":                           "glm-4.7",
		"google-gemma-4-31b-it":                     "gemma-4-31b-it",
		"anthropic-claude-opus-5-5-fast":            "claude-opus-5.5-fast",
		"nvidia-nemotron-3-nano-30b-a3b":            "nemotron-3-nano-30b-a3b",
		"xiaomi-mimo-v2-6-flash":                    "mimo-v2.6-flash",
		"aion-labs-aion-3-5-mini":                   "aion-3.5-mini",
		"deepseek-v4-1-flash-1017":                  "deepseek-v4.1-flash-1017",
		"e2ee-deepseek-v4-1-flash-1017-abliterated": "e2ee-deepseek-v4.1-flash-1017-abliterated",
		"e2ee-openai-gpt-61-sol":                    "e2ee-gpt-6.1-sol",
		"gemini-3-8-flash-lite":                     "gemini-3.8-flash-lite",
		"olafangensan-glm-4.7-flash-heretic":        "olafangensan-glm-4.7-flash-heretic",
		"unknown-model-61-fast":                     "unknown-model-61-fast",
	} {
		if got := normalizeVenice(id); got != want {
			t.Errorf("normalizeVenice(%q) = %q, want %q", id, got, want)
		}
		if got := normalizeVenice(want); got != want {
			t.Errorf("normalizing canonical %q changed it to %q", want, got)
		}
	}
}

func TestVeniceDiscoveryScopeAndOverrides(t *testing.T) {
	modelnames.SetEnabled(true)
	defer modelnames.SetEnabled(false)
	for _, tc := range []struct{ name, baseURL, want string }{
		{"Venice", "https://gateway.example/v1", "gpt-6.1-sol"},
		{"my-account", "https://api.venice.ai/api/v1", "gpt-6.1-sol"},
		{"other", "https://other.example/v1", "openai-gpt-61-sol"},
		{"other", "https://api.venice.ai.example/v1", "openai-gpt-61-sol"},
	} {
		compat := &config.OpenAICompatibility{
			Name: tc.name, BaseURL: tc.baseURL,
			ModelsDiscovery: &config.OpenAICompatibilityModelsDiscovery{Enabled: true},
		}
		got := MergeDiscovered(compat, []string{"openai-gpt-61-sol"})
		if len(got) != 1 || got[0].Name != "openai-gpt-61-sol" || got[0].Alias != tc.want {
			t.Fatalf("%s/%s: unexpected models %+v", tc.name, tc.baseURL, got)
		}
	}
	compat := &config.OpenAICompatibility{
		Name:            "venice",
		BaseURL:         "https://api.venice.ai/api/v1",
		Models:          []config.OpenAICompatibilityModel{{Name: "custom-upstream", Alias: "gpt-6.1-sol"}},
		ModelsDiscovery: &config.OpenAICompatibilityModelsDiscovery{Enabled: true},
	}
	if got := MergeDiscovered(compat, []string{"openai-gpt-61-sol", "gpt-6.1-sol"}); !reflect.DeepEqual(got, compat.Models) {
		t.Fatalf("configured alias must win: %+v", got)
	}
	compat.Models = nil
	if got := DiscoveredAliases(compat, []string{"openai-gpt-61-sol", "gpt-6.1-sol"}); !reflect.DeepEqual(got, []string{"gpt-6.1-sol"}) {
		t.Fatalf("catalog must deduplicate aliases: %v", got)
	}
	modelnames.SetEnabled(false)
	if got := DiscoveredAliases(compat, []string{"openai-gpt-61-sol"}); !reflect.DeepEqual(got, []string{"openai-gpt-61-sol"}) {
		t.Fatalf("disabled normalization must preserve IDs: %v", got)
	}
}
