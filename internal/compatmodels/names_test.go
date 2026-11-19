package compatmodels

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestNormalizeName(t *testing.T) {
	for id, want := range map[string]string{
		"gemini-3-8-flash":                          "gemini-3.8-flash",
		"gemini-3-5-flash-lite":                     "gemini-3.5-flash-lite",
		"gemini-3-flash-preview":                    "gemini-3-flash-preview",
		"openai-gpt-61-sol":                         "gpt-6.1-sol",
		"openai-gpt-6-astra-pro":                    "gpt-6-astra-pro",
		"openai-gpt-oss-120b":                       "gpt-oss-120b",
		"z-ai-glm-5-3-flash":                        "glm-5.3-flash",
		"zai-org-glm-4.7":                           "glm-4.7",
		"xiaomi-mimo-v2-6-flash":                    "mimo-v2.6-flash",
		"kimi-k3-1-pro-highspeed":                   "kimi-k3.1-pro-highspeed",
		"minimax-m27":                               "minimax-m2.7",
		"minimax-m3-1-flash":                        "minimax-m3.1-flash",
		"qwen3.8-flash":                             "qwen-3.8-flash",
		"qwen3-6-35b-a3b":                           "qwen-3.6-35b-a3b",
		"qwen4-max":                                 "qwen-4-max",
		"google-gemma-4-26b-a4b-it":                 "gemma-4-26b-a4b-it",
		"e2ee-deepseek-v4-1-flash-1017-abliterated": "e2ee-deepseek-v4.1-flash-1017-abliterated",
		"deepseek-v4-flash-0731":                    "deepseek-v4-flash-0731",
		"grok-4-20-multi-agent":                     "grok-4.20-multi-agent",
		"claude-opus-5-5-fast":                      "claude-opus-5.5-fast",
		"claude-opus-4-5-20251101":                  "claude-opus-4.5-20251101",
		"claude-3-5-haiku-20241022":                 "claude-3.5-haiku-20241022",
		"gemini-3.8-flash-high":                     "gemini-3.8-flash",
		"gemini-4-hydrogen-xhigh":                   "gemini-4-hydrogen",
		"gpt-oss-120b-medium":                       "gpt-oss-120b",
		"deepseek-v4.1-flash-1017":                  "deepseek-v4.1-flash-1017",
		"openai-gpt-4o-2024-11-20":                  "gpt-4o-2024-11-20",
		"deepseek-v4.1-flash":                       "deepseek-v4.1-flash",
		"glm-5.3":                                   "glm-5.3",
	} {
		if got := NormalizeName(id); got != want {
			t.Errorf("NormalizeName(%q) = %q, want %q", id, got, want)
		}
	}
}

func TestMergeDiscoveredNormalizesNames(t *testing.T) {
	compat := &config.OpenAICompatibility{
		Name:    "venice",
		BaseURL: "https://x/v1",
		Models: []config.OpenAICompatibilityModel{
			{Name: "gemini-3-8-flash-custom", Alias: "gemini-3.8-flash"},
		},
		ModelsDiscovery: &config.OpenAICompatibilityModelsDiscovery{
			Enabled: true, NormalizeNames: true, Exclude: []string{"e2ee-*"},
		},
	}
	discovered := []string{"e2ee-glm-5-3-p", "gemini-3-7-flash", "gemini-3-8-flash", "qwen-3-8-max", "qwen3-8-max"}
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

	compat.ModelsDiscovery.NormalizeNames = false
	if got := MergeDiscovered(compat, discovered); got[1].Alias != "gemini-3-7-flash" {
		t.Fatalf("without normalize-names discovered IDs keep their name, got %q", got[1].Alias)
	}
}
