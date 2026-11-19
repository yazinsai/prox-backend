package compatmodels

import (
	"regexp"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

// Vendor prefixes some aggregators put in front of an upstream model ID
// ("openai-gpt-61-sol"). Normalized names drop them so one model has one name
// across providers.
var vendorPrefixes = []string{"openai-", "z-ai-", "zai-org-", "google-", "xiaomi-", "nvidia-", "aion-labs-"}

var (
	datedID        = regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)
	squashedGPT    = regexp.MustCompile(`^gpt-(\d)(\d)(-|$)`)
	squashedMinMax = regexp.MustCompile(`^minimax-m(\d)(\d)(-|$)`)
	gluedQwen      = regexp.MustCompile(`^qwen(\d)`)
	versionMajor   = regexp.MustCompile(`^[a-z]?\d{1,2}$`)
	versionMinor   = regexp.MustCompile(`^\d{1,2}$`)
)

// NormalizeName rewrites a discovered model ID to the "family-major.minor-variant"
// convention: "gemini-3-8-flash" -> "gemini-3.8-flash", "openai-gpt-61-sol" ->
// "gpt-6.1-sol", "qwen3.8-max" -> "qwen-3.8-max". Claude IDs keep their upstream dashes, and
// dated snapshots only lose a vendor prefix. An "e2ee-" marker is preserved.
func NormalizeName(id string) string {
	id = strings.TrimSpace(id)
	lower := strings.ToLower(id)
	if lower == "" || strings.HasPrefix(lower, "claude-") {
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

// MergeDiscovered appends discovered IDs that the configured entries do not
// already expose. Configured entries keep their aliases and settings; a discovered
// ID is sent upstream unchanged and routed under its own name, or under its
// normalized name when the provider enables normalize-names. When several IDs
// normalize to one name, the first in catalog order wins.
func MergeDiscovered(compat *config.OpenAICompatibility, discovered []string) []config.OpenAICompatibilityModel {
	if compat == nil {
		return nil
	}
	if !compat.DiscoveryEnabled() || len(discovered) == 0 {
		return compat.Models
	}
	exposed := make(map[string]struct{}, len(compat.Models)+len(discovered))
	for _, model := range compat.Models {
		id := strings.TrimSpace(model.Alias)
		if id == "" {
			id = strings.TrimSpace(model.Name)
		}
		exposed[strings.ToLower(id)] = struct{}{}
	}
	normalize := compat.ModelsDiscovery.NormalizeNames
	out := append([]config.OpenAICompatibilityModel(nil), compat.Models...)
	for _, id := range discovered {
		if Excluded(compat.ModelsDiscovery.Exclude, id) {
			continue
		}
		alias := id
		if normalize {
			alias = NormalizeName(id)
		}
		_, idTaken := exposed[strings.ToLower(id)]
		_, aliasTaken := exposed[strings.ToLower(alias)]
		if idTaken || aliasTaken {
			continue
		}
		exposed[strings.ToLower(alias)] = struct{}{}
		out = append(out, config.OpenAICompatibilityModel{Name: id, Alias: alias})
	}
	return out
}

// DiscoveredAliases lists the names MergeDiscovered adds beyond the configured entries.
func DiscoveredAliases(compat *config.OpenAICompatibility, discovered []string) []string {
	if compat == nil {
		return nil
	}
	merged := MergeDiscovered(compat, discovered)
	out := make([]string, 0, len(merged))
	for _, model := range merged[len(compat.Models):] {
		out = append(out, model.Alias)
	}
	return out
}
