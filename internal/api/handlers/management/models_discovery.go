package management

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/compatmodels"
)

// GetDiscoveredModels reports the catalogs listed by OpenAI-compatible providers
// with models-discovery enabled. It reads cached state and never calls upstream.
//
//	GET /v8/management/routing/discovered-models
func (h *Handler) GetDiscoveredModels(c *gin.Context) {
	statuses := compatmodels.Default().Statuses(compatmodels.Targets(h.configSnapshot()))
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{"providers": statuses})
}
