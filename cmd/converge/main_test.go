package main

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"
)

func TestCLI_PrintUsage(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printUsage()

	_ = w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	output := buf.String()

	if len(output) == 0 {
		t.Fatal("expected usage output, got empty string")
	}
}

func TestCLI_RunDemoAuto(t *testing.T) {
	// Verify that runDemo executes all phases and completes without panicking
	done := make(chan struct{})
	go func() {
		runDemo([]string{"--auto", "--delay", "10ms"})
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(15 * time.Second):
		t.Fatal("demo timed out")
	}
}
