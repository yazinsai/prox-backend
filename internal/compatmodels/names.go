package compatmodels

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/modelnames"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

// MergeDiscovered appends discovered IDs that the configured entries do not
// already expose. Configured entries keep their aliases and settings; a discovered
// ID is sent upstream unchanged and routed under its own name, or under its
// normalized name when routing.normalize-model-names is on. When several IDs
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
	normalize := modelnames.Enabled()
	out := append([]config.OpenAICompatibilityModel(nil), compat.Models...)
	for _, id := range discovered {
		if Excluded(compat.ModelsDiscovery.Exclude, id) {
			continue
		}
		alias := id
		if normalize {
			alias = modelnames.Normalize(id)
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
