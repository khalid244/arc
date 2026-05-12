package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

const defaultRollupAPI = "http://localhost:8000"

// runRollupCLI dispatches `arc rollup <subcommand> [args...]`.
func runRollupCLI(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: arc rollup <list|describe|pause|resume|rebuild|propose> [args]")
		os.Exit(2)
	}
	apiBase := os.Getenv("ARC_API")
	if apiBase == "" {
		apiBase = defaultRollupAPI
	}
	switch args[0] {
	case "list":
		mustGET(apiBase + "/api/v1/rollups")
	case "describe":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: arc rollup describe <name>")
			os.Exit(2)
		}
		mustGET(apiBase + "/api/v1/rollups/" + args[1])
	case "pause", "resume", "rebuild":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "usage: arc rollup %s <name>\n", args[0])
			os.Exit(2)
		}
		mustPOST(apiBase + "/api/v1/rollups/" + args[1] + "/" + args[0])
	case "propose":
		if len(args) < 2 || !strings.Contains(args[1], ".") {
			fmt.Fprintln(os.Stderr, "usage: arc rollup propose <database>.<table>")
			os.Exit(2)
		}
		runPropose(args[1])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", args[0])
		os.Exit(2)
	}
}

func mustGET(url string) {
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "HTTP %d: %s\n", resp.StatusCode, body)
		os.Exit(1)
	}
	var v any
	if json.Unmarshal(body, &v) == nil {
		out, _ := json.MarshalIndent(v, "", "  ")
		fmt.Println(string(out))
		return
	}
	fmt.Println(string(body))
}

func mustPOST(url string) {
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "HTTP %d: %s\n", resp.StatusCode, body)
		os.Exit(1)
	}
	fmt.Println(string(body))
}

// runPropose is a stub during the v1 -> v2 rollup rework. The v1 column
// classifier (proposer.go) was deleted; the replacement is internal/rollup
// inference.go + specgen.go, which run from server startup against the live
// schema. A CLI front-end for inference is a Task 7 sub-task.
func runPropose(fq string) {
	_ = fq
	fmt.Fprintln(os.Stderr, "arc rollup propose: temporarily unavailable during v1->v2 rework.")
	fmt.Fprintln(os.Stderr, "v2 specs are auto-generated at server start from the inferred schema.")
	fmt.Fprintln(os.Stderr, "See docs/superpowers/specs/2026-05-10-rollup-tables-design.md.")
	os.Exit(2)
}
