package rollup

import (
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// HTTPHandler exposes the rollup management endpoints.
type HTTPHandler struct {
	Specs    []RollupSpec
	WMReader WMReader
	Builder  bool     // is this node the builder?
	Control  *Control // non-nil only when Builder is true
	Logger   zerolog.Logger
}

// Register attaches /api/v1/rollups routes to the Fiber app.
func (h *HTTPHandler) Register(app *fiber.App) {
	g := app.Group("/api/v1/rollups")
	g.Get("/", h.list)
	g.Get("/:name", h.describe)
	g.Post("/:name/pause", h.requireBuilder(h.pause))
	g.Post("/:name/resume", h.requireBuilder(h.resume))
	g.Post("/:name/rebuild", h.requireBuilder(h.rebuild))
}

func (h *HTTPHandler) findSpec(name string) *RollupSpec {
	for i := range h.Specs {
		if h.Specs[i].Name == name {
			return &h.Specs[i]
		}
	}
	return nil
}

func (h *HTTPHandler) list(c *fiber.Ctx) error {
	ctx := c.Context()
	out := make([]fiber.Map, 0, len(h.Specs))
	for _, s := range h.Specs {
		wm, err := h.WMReader.Get(ctx, s.Name)
		if err != nil {
			h.Logger.Warn().Err(err).Str("rollup", s.Name).Msg("failed to read watermark for list")
		}
		out = append(out, fiber.Map{
			"name":             s.Name,
			"database":         s.Database,
			"source_table":     s.SourceTable,
			"bucket_interval":  s.BucketInterval.String(),
			"watermark":        wm.Watermark,
			"last_completed":   wm.LastBuildCompletedAt,
			"is_paused":        h.Control != nil && h.Control.IsPaused(s.Name),
			"spec_fingerprint": s.Fingerprint(),
		})
	}
	return c.JSON(out)
}

func (h *HTTPHandler) describe(c *fiber.Ctx) error {
	s := h.findSpec(c.Params("name"))
	if s == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "rollup not found"})
	}
	wm, _ := h.WMReader.Get(c.Context(), s.Name)
	return c.JSON(fiber.Map{
		"spec":      s,
		"watermark": wm,
		"is_paused": h.Control != nil && h.Control.IsPaused(s.Name),
	})
}

func (h *HTTPHandler) pause(c *fiber.Ctx) error {
	name := c.Params("name")
	if h.findSpec(name) == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "rollup not found"})
	}
	h.Control.Pause(name)
	return c.JSON(fiber.Map{"name": name, "paused": true})
}

func (h *HTTPHandler) resume(c *fiber.Ctx) error {
	name := c.Params("name")
	if h.findSpec(name) == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "rollup not found"})
	}
	h.Control.Resume(name)
	return c.JSON(fiber.Map{"name": name, "paused": false})
}

func (h *HTTPHandler) rebuild(c *fiber.Ctx) error {
	name := c.Params("name")
	if h.findSpec(name) == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "rollup not found"})
	}
	h.Control.RequestRebuild(name)
	return c.JSON(fiber.Map{"name": name, "rebuild_requested": true})
}

func (h *HTTPHandler) requireBuilder(next fiber.Handler) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if !h.Builder {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error": "this node is not the rollup builder; pause/resume/rebuild must be sent to the builder node",
			})
		}
		return next(c)
	}
}
