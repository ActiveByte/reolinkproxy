//go:build unix

package main

import (
	"fmt"
	"os"
	"syscall"
)

// respawnSelf replaces the current process image in place via execve, keeping this
// process's PID. That matters when running as PID 1 in a container: fork+exit (the
// Windows approach in respawn_windows.go) would let the kernel's pid-namespace cleanup
// SIGKILL the freshly spawned child the instant this parent exits (see
// pid_namespaces(7): "If the process that is init(1) for a PID namespace terminates, the
// kernel terminates all of the processes in the namespace"), so the child never gets a
// chance to bind its ports. execve avoids that entirely - there's no parent/child and no
// exit, just a new program image loaded into the same process.
func respawnSelf() error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve own executable path: %w", err)
	}

	argv := append([]string{exe}, os.Args[1:]...)
	return syscall.Exec(exe, argv, os.Environ())
}
