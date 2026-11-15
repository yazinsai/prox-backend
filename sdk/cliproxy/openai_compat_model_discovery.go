package cliproxy

import (
	"context"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/compatmodels"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

const (
	// compatDiscoveryIdleWake bounds how long the scheduler sleeps with nothing due.
	// It only re-reads deadlines; requests are sent only for due catalogs.
	compatDiscoveryIdleWake = time.Hour
	// compatDiscoveryMinWake coalesces deadlines that fall close together.
	compatDiscoveryMinWake = 5 * time.Second
)

// withDiscoveredCompatModels appends discovered IDs that the configured entries do
// not already expose. Configured entries keep their aliases and settings; a
// discovered ID is routed under its own name and sent upstream unchanged.
func withDiscoveredCompatModels(compat *config.OpenAICompatibility, discovered []string) []config.OpenAICompatibilityModel {
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
	out := append([]config.OpenAICompatibilityModel(nil), compat.Models...)
	for _, id := range discovered {
		if _, ok := exposed[strings.ToLower(id)]; ok || compatmodels.Excluded(compat.ModelsDiscovery.Exclude, id) {
			continue
		}
		exposed[strings.ToLower(id)] = struct{}{}
		out = append(out, config.OpenAICompatibilityModel{Name: id, Alias: id})
	}
	return out
}

func compatDiscoveryTargets(cfg *config.Config) []compatmodels.Target {
	return compatmodels.Targets(cfg)
}

func (s *Service) compatDiscoveryWakeChan() chan struct{} {
	s.compatDiscoveryWakeOnce.Do(func() { s.compatDiscoveryWake = make(chan struct{}, 1) })
	return s.compatDiscoveryWake
}

func (s *Service) signalCompatModelDiscovery() {
	if s == nil {
		return
	}
	select {
	case s.compatDiscoveryWakeChan() <- struct{}{}:
	default:
	}
}

// runCompatModelDiscovery refreshes catalogs only when they are due: on first
// sight, after their interval, or after a failure's backoff. Config changes only
// re-read deadlines, so editing unrelated settings never triggers upstream calls.
func (s *Service) runCompatModelDiscovery(ctx context.Context) {
	wake := s.compatDiscoveryWakeChan()
	// published is owned by this goroutine: the catalog last pushed to the registry
	// per provider. Comparing against it, rather than only against refresh results,
	// also publishes a persisted list that loaded after the first registration.
	published := make(map[string]string)
	for {
		delay := s.refreshDueCompatModels(ctx, published)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (s *Service) refreshDueCompatModels(ctx context.Context, published map[string]string) time.Duration {
	store := compatmodels.Default()
	targets := compatDiscoveryTargets(s.currentConfig())
	store.Retain(targets)
	active := make(map[string]struct{}, len(targets))
	now := time.Now()
	next := now.Add(compatDiscoveryIdleWake)
	for _, target := range targets {
		if ctx.Err() != nil {
			return compatDiscoveryIdleWake
		}
		due := store.NextRefresh(target)
		if !due.After(now) {
			changed, errRefresh := store.Refresh(ctx, target)
			if ctx.Err() != nil {
				return compatDiscoveryIdleWake
			}
			compatmodels.LogRefresh(target.Name, changed, len(store.Models(target.Name, target.BaseURL)), errRefresh)
			due = store.NextRefresh(target)
		}
		key := strings.ToLower(strings.TrimSpace(target.Name))
		active[key] = struct{}{}
		if fingerprint := strings.Join(store.Models(target.Name, target.BaseURL), "\n"); fingerprint != published[key] {
			s.republishCompatModels(ctx, target.Name)
			published[key] = fingerprint
		}
		if due.Before(next) {
			next = due
		}
	}
	// A disabled provider is re-registered by the config reload itself; forget it so
	// re-enabling publishes again.
	for key := range published {
		if _, ok := active[key]; !ok {
			delete(published, key)
		}
	}
	return max(time.Until(next), compatDiscoveryMinWake)
}

// republishCompatModels re-registers the provider's credentials so routing sees
// the new catalog. Registration reads the store through buildOpenAICompatibilityConfigModels.
func (s *Service) republishCompatModels(ctx context.Context, name string) {
	if s == nil || s.coreManager == nil {
		return
	}
	for _, auth := range s.coreManager.List() {
		if auth == nil || auth.Disabled || !compatAuthBelongsTo(auth, name) {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		s.refreshModelRegistrationForAuthWithContext(ctx, auth, nil)
	}
}

func compatAuthBelongsTo(auth *coreauth.Auth, name string) bool {
	if auth.Attributes == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(auth.Attributes["compat_name"]), strings.TrimSpace(name))
}
