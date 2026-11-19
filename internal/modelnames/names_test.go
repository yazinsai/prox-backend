package modelnames

import "testing"

func TestLegacyRequest(t *testing.T) {
	known := map[string]bool{"claude-opus-5.5": true, "gpt-6.1-sol": true, "gemini-3.8-flash": true}
	isKnown := func(model string) bool { return known[model] }
	SetEnabled(true)
	defer SetEnabled(false)
	for model, want := range map[string]string{
		"claude-opus-5-5":          "claude-opus-5.5",
		"claude-opus-5-5(high)":    "claude-opus-5.5(high)",
		"gemini-3.8-flash-high":    "gemini-3.8-flash",
		"gpt-6.1-sol":              "",
		"claude-sonnet-5-5":        "",
		"openai-gpt-61-sol(xhigh)": "gpt-6.1-sol(xhigh)",
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
