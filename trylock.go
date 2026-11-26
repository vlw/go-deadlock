package deadlock

import "github.com/petermattis/goid"

// TryLock tries to lock the mutex.
// Returns false if the lock is already in use, true otherwise.
func (m *Mutex) TryLock() bool {
	return trylock(m.mu.TryLock, m.mu.Unlock, m)
}

// TryLock tries to lock rw for writing.
// Returns false if the lock is already locked for reading or writing, true otherwise.
func (m *RWMutex) TryLock() bool {
	return trylock(m.mu.TryLock, m.mu.Unlock, m)
}

// TryRLock tries to lock rw for reading.
// Returns false if the lock is already locked for writing, true otherwise.
func (m *RWMutex) TryRLock() bool {
	return trylock(m.mu.TryRLock, m.mu.RUnlock, m)
}

// trylock cannot block, so it skips deadlock timeout detection while still
// checking lock ordering and recording the holder when acquisition succeeds.
// Failed attempts do not call postUnlock, avoiding corrupting another
// goroutine's holder entry.
func trylock(lockFn func() bool, unlockFn func(), ptr interface{}) bool {
	if Opts.Disable {
		return lockFn()
	}
	if !lockFn() {
		return false
	}
	gid := goid.Get()
	stack, buf := callers(1)
	violations := lo.preAndPostLock(gid, stack, buf, ptr)
	if violations > 0 {
		panicked := true
		defer func() {
			if panicked {
				lo.postUnlock(ptr)
				unlockFn()
			}
		}()
		for i := 0; i < violations; i++ {
			Opts.OnPotentialDeadlock()
		}
		panicked = false
	}
	return true
}
