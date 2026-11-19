package modelnames

import "testing"

func TestLegacyRequest(t *testing.T) {
	known := map[string]bool{"claude-opus-5.5": true, "gpt-6.1-sol": true, "gemini-3.8-flash": true}
	isKnown := func(model string) bool { return known[model] }
	SetEnabled(true)
	defer SetEnabled(false)
	for model, want := range map[string]string{
		"claude-opus-5-5":       "claude-opus-5.5",
		"claude-opus-5-5(high)": "claude-opus-5.5(high)",
		"gemini-3.8-flash-high": "gemini-3.8-flash",
		"gpt-6.1-sol":           "",
		"claude-sonnet-5-5":     "",
	} {
		if got := LegacyRequest(model, isKnown); got != want {
			t.Errorf("LegacyRequest(%q) = %q, want %q", model, got, want)
		}
	}
	SetEnabled(false)
	if got := LegacyRequest("claude-opus-5-5", isKnown); got != "" {
		t.Errorf("disabled LegacyRequest = %q", got)
	}
}

func TestRecordOriginal(t *testing.T) {
	RecordOriginal("Claude", "claude-opus-5.5", "claude-opus-5-5")
	if got := Original("claude", "CLAUDE-OPUS-5.5"); got != "claude-opus-5-5" {
		t.Fatalf("Original = %q", got)
	}
	if got := Original("codex", "claude-opus-5.5"); got != "" {
		t.Fatalf("other channel Original = %q", got)
	}
}

func TestNormalize(t *testing.T) {
	for id, want := range map[string]string{
		"gemini-3-8-flash":         "gemini-3.8-flash",
		"gemini-3-5-flash-lite":    "gemini-3.5-flash-lite",
		"gemini-3-flash-preview":   "gemini-3-flash-preview",
		"gemini-3.8-flash-high":    "gemini-3.8-flash",
		"gemini-4-hydrogen-xhigh":  "gemini-4-hydrogen",
		"gpt-oss-120b-medium":      "gpt-oss-120b",
		"kimi-k3-1-pro-highspeed":  "kimi-k3.1-pro-highspeed",
		"qwen3.8-flash":            "qwen-3.8-flash",
		"qwen3-6-35b-a3b":          "qwen-3.6-35b-a3b",
		"e2ee-qwen3-8-27b":         "e2ee-qwen-3.8-27b",
		"glm-5v-turbo":             "glm-5v-turbo",
		"hy4-flash-free":           "hy4-flash-free",
		"deepseek-v4-1-flash-1017": "deepseek-v4.1-flash-1017",
		"deepseek-v4.1-flash-1017": "deepseek-v4.1-flash-1017",
		"claude-opus-5-5":          "claude-opus-5.5",
		"claude-opus-5-5-fast":     "claude-opus-5.5-fast",
		"claude-opus-4-5-20251101": "claude-opus-4.5-20251101",
		"gpt-4o-2024-11-20":        "gpt-4o-2024-11-20",
		"gpt-6.1-sol":              "gpt-6.1-sol",
	} {
		if got := Normalize(id); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", id, got, want)
		}
	}
}

func TestDisplayName(t *testing.T) {
	for in, want := range map[string]string{
		"Claude Opus 5.5 (High)":            "Claude Opus 5.5",
		"GPT-OSS 120B (Medium)":             "GPT-OSS 120B",
		"Gemini 3.8 Flash (xhigh)":          "Gemini 3.8 Flash",
		"deepseek-v4.1-Flash (Abliterated)": "deepseek-v4.1-Flash (Abliterated)",
		"Claude Opus 5.5":                   "Claude Opus 5.5",
	} {
		if got := DisplayName(in); got != want {
			t.Errorf("DisplayName(%q) = %q, want %q", in, got, want)
		}
	}
}
