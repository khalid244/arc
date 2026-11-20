//go:build !windows

package main

import (
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Each of SIGINT, SIGTERM and SIGHUP cancels the run's context.
func TestCompactLocalSignalContextCancels(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP} {
		if signal.Ignored(sig) {
			// go test itself runs under nohup / in a background job: the
			// run keeps such a signal ignored (see the test below).
			t.Logf("%v is ignored in this process; not sent", sig)
			continue
		}
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
	if signal.Ignored(syscall.SIGINT) {
		t.Skip("SIGINT is ignored in this process, so in the child too")
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

// Started with SIGHUP and SIGINT ignored (nohup, or `cmd &` in a script), the
// run keeps them ignored: a hangup or a Ctrl-C meant for the terminal must
// not cancel it. SIGTERM still does.
func TestCompactLocalKeepsIgnoredSignalsIgnored(t *testing.T) {
	if os.Getenv("ARC_COMPACT_LOCAL_SIGNAL_CHILD") == "ignored" {
		ctx, stop := compactLocalSignalContext()
		defer stop()
		os.Stdout.WriteString("ready\n")
		select {
		case <-ctx.Done():
			os.Stdout.WriteString("cancelled\n")
			os.Exit(3)
		case <-time.After(3 * time.Second):
			os.Stdout.WriteString("survived\n")
			os.Exit(0)
		}
	}
	for _, tc := range []struct {
		name     string
		send     []syscall.Signal
		wantCode int
	}{
		{"HUP and INT are ignored", []syscall.Signal{syscall.SIGHUP, syscall.SIGINT}, 0},
		{"TERM still cancels", []syscall.Signal{syscall.SIGTERM}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// trap '' sets SIG_IGN, which exec keeps: the child starts the way
			// nohup starts a command.
			cmd := exec.Command("/bin/sh", "-c", `trap '' HUP INT; exec "$0" -test.run='^TestCompactLocalKeepsIgnoredSignalsIgnored$'`, os.Args[0])
			cmd.Env = append(os.Environ(), "ARC_COMPACT_LOCAL_SIGNAL_CHILD=ignored")
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 256)
			got := ""
			deadline := time.Now().Add(10 * time.Second)
			for !strings.Contains(got, "ready") {
				if time.Now().After(deadline) {
					_ = cmd.Process.Kill()
					t.Fatalf("child never got ready (%q)", got)
				}
				n, _ := out.Read(buf)
				got += string(buf[:n])
			}
			for _, sig := range tc.send {
				if err := cmd.Process.Signal(sig); err != nil {
					t.Fatal(err)
				}
			}
			for {
				n, err := out.Read(buf)
				got += string(buf[:n])
				if err != nil {
					break
				}
			}
			_ = cmd.Wait()
			if code := cmd.ProcessState.ExitCode(); code != tc.wantCode {
				t.Fatalf("child exit code %d (%v), output %q; want %d", code, cmd.ProcessState, got, tc.wantCode)
			}
		})
	}
}
