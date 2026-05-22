//go:build windows

package secrets

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func lockKeyringFile(file *os.File, exclusive bool) error {
	flags := uint32(windows.LOCKFILE_FAIL_IMMEDIATELY)
	if exclusive {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}

	var overlapped windows.Overlapped
	if err := windows.LockFileEx(windows.Handle(file.Fd()), flags, 0, 1, 0, &overlapped); err != nil {
		return fmt.Errorf("lock keyring file: %w", err)
	}
	return nil
}

func unlockKeyringFile(file *os.File) error {
	var overlapped windows.Overlapped
	if err := windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, &overlapped); err != nil {
		return fmt.Errorf("unlock keyring file: %w", err)
	}
	return nil
}

func keyringLockWouldBlock(err error) bool {
	return errors.Is(err, windows.ERROR_LOCK_VIOLATION)
}
