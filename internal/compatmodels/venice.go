package compatmodels

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/modelnames"
)

var (
	veniceVendor         = regexp.MustCompile(`^(openai|anthropic|google|z-ai|zai-org|nvidia|xiaomi|aion-labs)-`)
	veniceCompactVersion = regexp.MustCompile(`^(gpt-|minimax-m)([0-9])([0-9])(-|$)`)
)

func isVenice(compat *config.OpenAICompatibility) bool {
	endpoint, err := url.Parse(strings.TrimSpace(compat.BaseURL))
	return strings.EqualFold(strings.TrimSpace(compat.Name), "venice") ||
		(err == nil && strings.EqualFold(endpoint.Hostname(), "api.venice.ai"))
}

// normalizeVenice removes Venice's vendor namespaces and expands its two-digit
// GPT/MiniMax versions before applying the shared rules. Other families and
// unknown formats are left to the shared normalizer; variants remain distinct.
func normalizeVenice(id string) string {
	name := strings.ToLower(strings.TrimSpace(id))
	prefix := ""
	if strings.HasPrefix(name, "e2ee-") {
		prefix, name = "e2ee-", strings.TrimPrefix(name, "e2ee-")
	}
	name = veniceVendor.ReplaceAllString(name, "")
	name = veniceCompactVersion.ReplaceAllString(name, "${1}${2}.${3}${4}")
	return prefix + modelnames.Normalize(name)
}
