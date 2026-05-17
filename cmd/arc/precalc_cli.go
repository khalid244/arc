package main

import (
	"context"
	"fmt"
	"os"
)

func runPrecalcCLI(args []string) {
	if len(args) == 0 {
		printPrecalcHelp()
		return
	}
	switch args[0] {
	case "backfill":
		runPrecalcBackfill(context.Background(), args[1:])
	case "status":
		runPrecalcStatus(context.Background(), args[1:])
	case "help", "-h", "--help":
		printPrecalcHelp()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n\n", args[0])
		printPrecalcHelp()
		os.Exit(2)
	}
}

func printPrecalcHelp() {
	fmt.Println("Usage: arc precalc <subcommand> [args]")
	fmt.Println("")
	fmt.Println("Subcommands:")
	fmt.Println("  backfill <table>   Classify the table and build all tiers + variants from the full source range")
	fmt.Println("  status <table>     Print per-(tier, variant) watermark, file count, and total bytes")
	fmt.Println("")
}

func runPrecalcBackfill(ctx context.Context, args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: arc precalc backfill <table>")
		os.Exit(2)
	}
	fmt.Fprintf(os.Stderr, "backfill: not yet implemented (table=%s)\n", args[0])
	os.Exit(2)
}

func runPrecalcStatus(ctx context.Context, args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: arc precalc status <table>")
		os.Exit(2)
	}
	fmt.Fprintf(os.Stderr, "status: not yet implemented (table=%s)\n", args[0])
	os.Exit(2)
}
