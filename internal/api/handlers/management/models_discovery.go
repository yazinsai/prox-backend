package management

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/compatmodels"
)

// GetDiscoveredModels reports the catalogs listed by OpenAI-compatible providers
// with models-discovery enabled. Each provider's models are the names discovery
// adds to routing (normalized when normalize-names is on, without IDs the
// configured models already expose). It reads cached state and never calls upstream.
//
//	GET /v8/management/routing/discovered-models
func (h *Handler) GetDiscoveredModels(c *gin.Context) {
	cfg := h.configSnapshot()
	statuses := compatmodels.Default().Statuses(compatmodels.Targets(cfg))
	if cfg != nil {
		for i := range statuses {
			for j := range cfg.OpenAICompatibility {
				compat := &cfg.OpenAICompatibility[j]
				if compat.DiscoveryEnabled() && strings.EqualFold(strings.TrimSpace(compat.Name), statuses[i].Name) {
					statuses[i].Models = compatmodels.DiscoveredAliases(compat, statuses[i].Models)
					break
				}
			}
		}
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{"providers": statuses})
}
