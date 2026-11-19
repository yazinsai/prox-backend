// Package modelnames normalizes model IDs to one naming convention so a model has
// the same name across providers.
package modelnames

import (
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
)

// Reasoning effort is a request setting, so an effort suffix in the name is dropped.
var effortSuffixes = map[string]bool{"minimal": true, "low": true, "medium": true, "high": true, "xhigh": true}

var (
	datedID      = regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)
	gluedVersion = regexp.MustCompile(`^([a-z]{3,})(\d.*)$`)
	versionMajor = regexp.MustCompile(`^[a-z]?\d{1,2}$`)
	versionMinor = regexp.MustCompile(`^\d{1,2}$`)
)

// Normalize rewrites a model ID to "family-major.minor-variant" with three generic
// rules: adjacent short version numbers are joined ("gemini-3-8-flash" ->
// "gemini-3.8-flash", "kimi-k3-1-pro" -> "kimi-k3.1-pro"), a family glued to its
// version is split ("qwen3.8-max" -> "qwen-3.8-max"), and a trailing effort level
// is dropped ("gemini-3.8-flash-high" -> "gemini-3.8-flash"). Longer numbers such
// as "-1017" and dated "YYYY-MM-DD" snapshots are left alone.
func Normalize(id string) string {
	name := strings.ToLower(strings.TrimSpace(id))
	if name == "" || datedID.MatchString(name) {
		return name
	}
	var tokens []string
	for _, token := range strings.Split(name, "-") {
		if parts := gluedVersion.FindStringSubmatch(token); parts != nil {
			tokens = append(tokens, parts[1], parts[2])
			continue
		}
		tokens = append(tokens, token)
	}
	if n := len(tokens); n > 2 && effortSuffixes[tokens[n-1]] {
		tokens = tokens[:n-1]
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
	return strings.Join(out, "-")
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
