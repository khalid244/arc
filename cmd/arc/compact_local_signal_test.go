//go:build !windows

package main

import (
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Each of SIGINT, SIGTERM and SIGHUP cancels the run's context.
func TestCompactLocalSignalContextCancels(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP} {
		ctx, stop := compactLocalSignalContext()
		if err := syscall.Kill(os.Getpid(), sig); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("%v did not cancel the context", sig)
		}
		stop()
	}
}

// After the first signal the handler is gone, so a second Ctrl-C kills the
// process at once (a COPY that ignores the interrupt can still be stopped).
// Runs in a child process: the second signal must terminate it.
func TestCompactLocalSecondSignalKills(t *testing.T) {
	if os.Getenv("ARC_COMPACT_LOCAL_SIGNAL_CHILD") == "1" {
		ctx, stop := compactLocalSignalContext()
		defer stop()
		os.Stdout.WriteString("ready\n")
		<-ctx.Done()
		os.Stdout.WriteString("cancelled\n")
		time.Sleep(10 * time.Second) // a stuck run; the second SIGINT must end it
		os.Exit(0)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestCompactLocalSecondSignalKills$")
	cmd.Env = append(os.Environ(), "ARC_COMPACT_LOCAL_SIGNAL_CHILD=1")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	read := func(want string) {
		deadline := time.Now().Add(10 * time.Second)
		got := ""
		for !strings.Contains(got, want) {
			if time.Now().After(deadline) {
				t.Fatalf("child never printed %q (got %q)", want, got)
			}
			n, _ := out.Read(buf)
			got += string(buf[:n])
		}
	}
	read("ready")
	_ = cmd.Process.Signal(syscall.SIGINT)
	read("cancelled")
	_ = cmd.Process.Signal(syscall.SIGINT)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
		if err == nil || !ok || !ws.Signaled() || ws.Signal() != syscall.SIGINT {
			t.Fatalf("child exit = %v (%v), want killed by the second SIGINT", err, cmd.ProcessState)
		}
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("the second SIGINT did not kill the child")
	}
}
