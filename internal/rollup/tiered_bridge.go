package rollup

import "github.com/basekick-labs/arc/internal/rollup/tiered"

// ConvertConfig translates the viper-bound rollup.Config into the internal
// tiered.Config used by the tiered subsystem.
//
// Defaults are NOT applied — call result.Defaults() afterward if zero
// fields should be filled with built-in values.
func ConvertConfig(c Config) tiered.Config {
	out := tiered.Config{
		Enabled:           c.Enabled,
		TZ:                c.TZ,
		Builder:           c.Builder,
		Tiers:             append([]string(nil), c.Tiers...),
		GraceWindow:       c.GraceWindow,
		CoverageThreshold: c.CoverageThreshold,
		DimRichCap:        c.DimRichCap,
		HLLLgK:            c.HLLLgK,
		KLLk:              c.KLLk,
		ObsoleteGrace:     c.ObsoleteGrace,
	}
	if len(c.Tables) > 0 {
		out.Tables = make(map[string]tiered.TableOverride, len(c.Tables))
		for k, v := range c.Tables {
			out.Tables[k] = tiered.TableOverride{
				TimeColumn:  v.TimeColumn,
				ForceKeep:   append([]string(nil), v.ForceKeep...),
				ForceSketch: append([]string(nil), v.ForceSketch...),
				IgnoreCols:  append([]string(nil), v.IgnoreCols...),
			}
		}
	}
	return out
}

// ConvertTieredConfig is a deprecated alias for ConvertConfig. Retained so
// any old callers keep compiling during the migration window.
func ConvertTieredConfig(c Config) tiered.Config { return ConvertConfig(c) }
