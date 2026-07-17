//go:build windows

package main

import (
	"fmt"
	"os"
	"os/exec"
)

// respawnSelf launches a fresh copy of this process with the same arguments and
// environment, then returns without waiting for it - the current process is about to exit,
// having already released every port it held (runApp's deferred Shutdown calls have already
// run by the time main() reaches this point). Windows has no execve-style in-place process
// replacement, so unlike the Unix build (see respawn_unix.go) this spawns a child and lets
// the current process exit afterward - safe here since Windows never runs this as a
// container's PID 1.
func respawnSelf() error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve own executable path: %w", err)
	}

	cmd := exec.Command(exe, os.Args[1:]...)
	cmd.Env = os.Environ()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start replacement process: %w", err)
	}
	return nil
}
