package api

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// ReorgHandler exposes the late-event reorganizer over HTTP, mirroring
// CompactionHandler: a read-only status route plus an admin-gated trigger that
// kicks off one drain pass immediately instead of waiting for the cron.
type ReorgHandler struct {
	reorg       *compaction.Reorganizer
	authManager *auth.AuthManager
	logger      zerolog.Logger
}

// NewReorgHandler creates a new reorg handler.
func NewReorgHandler(reorg *compaction.Reorganizer, authManager *auth.AuthManager, logger zerolog.Logger) *ReorgHandler {
	return &ReorgHandler{
		reorg:       reorg,
		authManager: authManager,
		logger:      logger.With().Str("component", "reorg-handler").Logger(),
	}
}

// RegisterRoutes registers reorg endpoints.
func (h *ReorgHandler) RegisterRoutes(app *fiber.App) {
	group := app.Group("/api/v1/reorg")

	// Read-only route — any authenticated token.
	group.Get("/status", h.getStatus)

	// Admin route — triggering a drain requires admin permission (same gate as
	// compaction's /trigger).
	if h.authManager != nil {
		group.Post("/trigger", auth.RequireAdmin(h.authManager), h.triggerReorg)
	} else {
		group.Post("/trigger", h.triggerReorg)
	}

	h.logger.Info().Msg("Reorg routes registered")
}

// getStatus handles GET /api/v1/reorg/status.
func (h *ReorgHandler) getStatus(c *fiber.Ctx) error {
	if h.reorg == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Reorg not initialized",
		})
	}
	return c.JSON(fiber.Map{
		"is_running": h.reorg.IsRunning(),
	})
}

// triggerReorg handles POST /api/v1/reorg/trigger — runs one drain pass now.
// Takes no parameters: the reorganizer drains every configured late measurement
// across all databases, oldest bucket first.
func (h *ReorgHandler) triggerReorg(c *fiber.Ctx) error {
	if h.reorg == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Reorg not initialized",
		})
	}

	// Fast-fail if a drain is already in flight (cron or a prior trigger). The
	// Reorganizer's own CAS guard is the authoritative protection — this just
	// gives the caller an immediate 409 instead of a silent no-op.
	if h.reorg.IsRunning() {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error":      "Reorg drain already running",
			"message":    "A reorg drain pass is already in progress. Please wait for it to complete.",
			"is_running": true,
		})
	}

	h.logger.Info().Msg("Manual reorg drain triggered via API")

	// Run asynchronously. An operator-initiated backlog drain can be very long
	// (millions of late files across many buckets), so use a wide deadline — far
	// past the scheduled cycle_timeout. The Reorganizer's run-guard prevents
	// overlap with the cron, so the pod can't end up running two drains at once.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
		defer cancel()

		start := time.Now()
		err := h.reorg.Run(ctx)
		duration := time.Since(start)
		if err != nil {
			h.logger.Error().Err(err).Dur("duration", duration).Msg("Manual reorg drain failed")
		} else {
			h.logger.Info().Dur("duration", duration).Msg("Manual reorg drain completed")
		}
	}()

	return c.JSON(fiber.Map{
		"message": "Reorg drain triggered",
		"status":  "running",
	})
}
