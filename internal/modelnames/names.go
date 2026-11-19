// Package modelnames normalizes model IDs to one naming convention so a model has
// the same name across providers.
package modelnames

import (
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
)

// Vendor prefixes some aggregators put in front of an upstream model ID
// ("openai-gpt-61-sol").
var vendorPrefixes = []string{"openai-", "z-ai-", "zai-org-", "google-", "xiaomi-", "nvidia-", "aion-labs-"}

// Reasoning effort is a request setting, so an effort suffix in the name is dropped.
var effortSuffixes = map[string]struct{}{"minimal": {}, "low": {}, "medium": {}, "high": {}, "xhigh": {}}

var (
	datedID        = regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)
	squashedGPT    = regexp.MustCompile(`^gpt-(\d)(\d)(-|$)`)
	squashedMinMax = regexp.MustCompile(`^minimax-m(\d)(\d)(-|$)`)
	gluedQwen      = regexp.MustCompile(`^qwen(\d)`)
	versionMajor   = regexp.MustCompile(`^[a-z]?\d{1,2}$`)
	versionMinor   = regexp.MustCompile(`^\d{1,2}$`)
)

// Normalize rewrites a model ID to the "family-major.minor-variant" convention:
// "gemini-3-8-flash" -> "gemini-3.8-flash", "openai-gpt-61-sol" -> "gpt-6.1-sol",
// "claude-opus-5-5-fast" -> "claude-opus-5.5-fast", "gemini-3.8-flash-high" ->
// "gemini-3.8-flash". Only one- and two-digit pairs are joined, so release tags
// such as "-1017" or "-20251101" stay, and dated "YYYY-MM-DD" snapshots only lose
// a vendor prefix. An "e2ee-" marker is preserved.
func Normalize(id string) string {
	id = strings.TrimSpace(id)
	lower := strings.ToLower(id)
	if lower == "" {
		return id
	}
	marker := ""
	if strings.HasPrefix(lower, "e2ee-") {
		marker, lower = "e2ee-", lower[len("e2ee-"):]
	}
	for _, prefix := range vendorPrefixes {
		lower = strings.TrimPrefix(lower, prefix)
	}
	if datedID.MatchString(lower) {
		return marker + lower
	}
	lower = squashedGPT.ReplaceAllString(lower, "gpt-$1.$2$3")
	lower = squashedMinMax.ReplaceAllString(lower, "minimax-m$1.$2$3")
	lower = gluedQwen.ReplaceAllString(lower, "qwen-$1")

	tokens := strings.Split(lower, "-")
	if n := len(tokens); n > 2 {
		if _, ok := effortSuffixes[tokens[n-1]]; ok {
			tokens = tokens[:n-1]
		}
	}
	out := make([]string, 0, len(tokens))
	for i := 0; i < len(tokens); i++ {
		if i+1 < len(tokens) && versionMajor.MatchString(tokens[i]) && versionMinor.MatchString(tokens[i+1]) {
			out = append(out, tokens[i]+"."+tokens[i+1])
			i++
			continue
		}
		out = append(out, tokens[i])
	}
	return marker + strings.Join(out, "-")
}

var enabled atomic.Bool

// SetEnabled records routing.normalize-model-names for request-time lookups.
func SetEnabled(on bool) { enabled.Store(on) }

// Enabled reports whether model names are normalized.
func Enabled() bool { return enabled.Load() }

// LegacyRequest maps a request for a pre-normalization name ("claude-opus-5-5")
// to its normalized name when only the latter is routable. known reports whether
// a base model name is routable. The thinking suffix is preserved. It returns
// "" when no mapping applies.
func LegacyRequest(model string, known func(string) bool) string {
	if !Enabled() || known == nil {
		return ""
	}
	model = strings.TrimSpace(model)
	base, suffix := model, ""
	if open := strings.LastIndex(model, "("); open > 0 && strings.HasSuffix(model, ")") {
		base, suffix = model[:open], model[open:]
	}
	if known(base) {
		return ""
	}
	normalized := Normalize(base)
	if strings.EqualFold(normalized, base) || !known(normalized) {
		return ""
	}
	return normalized + suffix
}

var (
	originalsMu sync.RWMutex
	originals   = map[string]map[string]string{}
)

// RecordOriginal remembers that channel lists original under normalized, so a
// request for the normalized name can be sent upstream under the original.
func RecordOriginal(channel, normalized, original string) {
	channel, key := strings.ToLower(strings.TrimSpace(channel)), strings.ToLower(strings.TrimSpace(normalized))
	if channel == "" || key == "" || strings.EqualFold(normalized, original) {
		return
	}
	originalsMu.Lock()
	defer originalsMu.Unlock()
	if originals[channel] == nil {
		originals[channel] = map[string]string{}
	}
	originals[channel][key] = strings.TrimSpace(original)
}

// Original returns the name channel lists under normalized, or "".
func Original(channel, normalized string) string {
	originalsMu.RLock()
	defer originalsMu.RUnlock()
	return originals[strings.ToLower(strings.TrimSpace(channel))][strings.ToLower(strings.TrimSpace(normalized))]
}
