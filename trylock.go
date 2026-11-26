package deadlock

import (
	"unsafe"

	"github.com/petermattis/goid"
)

// TryLock tries to lock the mutex.
// Returns false if the lock is already in use, true otherwise.
// Failed attempts do not call postUnlock, avoiding corrupting another
// goroutine's holder entry.
func (m *Mutex) TryLock() bool {
	if !m.mu.TryLock() {
		return false
	}
	if !Opts.Disable {
		if violations := lo.preAndPostLock(goid.Get(), unsafe.Pointer(m)); violations > 0 {
			handleViolations(violations, unsafe.Pointer(m), m.mu.Unlock)
		}
	}
	return true
}

// TryLock tries to lock rw for writing.
// Returns false if the lock is already locked for reading or writing, true otherwise.
func (m *RWMutex) TryLock() bool {
	if !m.mu.TryLock() {
		return false
	}
	if !Opts.Disable {
		if violations := lo.preAndPostLock(goid.Get(), unsafe.Pointer(m)); violations > 0 {
			handleViolations(violations, unsafe.Pointer(m), m.mu.Unlock)
		}
	}
	return true
}

// TryRLock tries to lock rw for reading.
// Returns false if the lock is already locked for writing, true otherwise.
func (m *RWMutex) TryRLock() bool {
	if !m.mu.TryRLock() {
		return false
	}
	if !Opts.Disable {
		if violations := lo.preAndPostLock(goid.Get(), unsafe.Pointer(m)); violations > 0 {
			handleViolations(violations, unsafe.Pointer(m), m.mu.RUnlock)
		}
	}
	return true
}
