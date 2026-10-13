package api

import (
	"bytes"
	"io"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// StagerImporter stores a staged unit handed over by a terminating peer pod
// (ingest.FreshStager / ingest.LateStager).
type StagerImporter interface {
	ImportUnit(r io.Reader) (ingest.StagerImportResult, error)
}

// RegisterStagerImportRoute registers POST /api/v1/internal/stager/import
// (ingest.stager_import_enabled). A terminating ingest pod whose stagers could
// not upload during an object-store outage sends each remaining unit (a merge
// job with its sources, or a batch of staged files) here through the ingest
// Service, and deletes it locally only after a 2xx. The unit is routed by the
// X-Arc-Stager-Kind header to this node's stager of that kind, which stores it
// atomically and idempotently and uploads it when the store recovers.
//
// admit (optional, e.g. ArrowBuffer.CheckIngestGate) refuses the import with
// 503 while this node refuses writes itself, for instance above its local
// disk hard watermark, so the sender keeps the unit (or reaches another peer)
// rather than pushing this node over its budget. The route is cluster
// internal: protect it with a NetworkPolicy, and with auth when enabled.
func RegisterStagerImportRoute(app *fiber.App, importers map[string]StagerImporter, admit func() error, logger zerolog.Logger) {
	log := logger.With().Str("component", "stager-import").Logger()
	app.Post(ingest.StagerImportPath, func(c *fiber.Ctx) error {
		kind := c.Get(ingest.StagerKindHeader)
		imp, ok := importers[kind]
		if !ok || imp == nil {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error": "no " + kind + " stager on this node",
			})
		}
		if admit != nil {
			if err := admit(); err != nil {
				c.Set(fiber.HeaderRetryAfter, ingestBackpressureRetryAfter)
				return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
					"error": "import refused: " + err.Error(),
				})
			}
		}
		res, err := imp.ImportUnit(bytes.NewReader(c.Body()))
		if err != nil {
			status := ingest.StagerImportStatus(err)
			log.Error().Err(err).Str("kind", kind).Int("status", status).Str("remote", c.IP()).
				Msg("Stager import refused; the sender keeps the unit")
			return c.Status(status).JSON(fiber.Map{"error": err.Error()})
		}
		log.Info().Str("kind", kind).Int("stored", res.Stored).Int("skipped", res.Skipped).Str("remote", c.IP()).
			Msg("Stored staged unit handed over by a peer pod")
		return c.Status(fiber.StatusOK).JSON(res)
	})
}
