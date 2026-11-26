package deadlock

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestOnPotentialDeadlockDoesNotBlockOtherMutexes verifies that a slow
// OnPotentialDeadlock callback does not stall unrelated mutexes in other goroutines.
func TestOnPotentialDeadlockDoesNotBlockOtherMutexes(t *testing.T) {
	defer restore()()
	Opts.DeadlockTimeout = 0

	var a, b, unrelated Mutex
	// Record initial lock ordering a -> b.
	a.Lock()
	b.Lock()
	b.Unlock()
	a.Unlock()

	inCallback := make(chan struct{})
	releaseCallback := make(chan struct{})
	Opts.OnPotentialDeadlock = func() {
		close(inCallback)
		<-releaseCallback
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Lock()
		a.Lock()
		a.Unlock()
		b.Unlock()
	}()

	<-inCallback

	unrelatedDone := make(chan struct{})
	go func() {
		unrelated.Lock()
		unrelated.Unlock()
		close(unrelatedDone)
	}()

	select {
	case <-unrelatedDone:
	case <-time.After(300 * time.Millisecond):
		close(releaseCallback)
		<-done
		<-unrelatedDone
		t.Fatal("unrelated mutex was blocked while OnPotentialDeadlock callback was running")
	}
	close(releaseCallback)
	<-done
}

// TestOnPotentialDeadlockPanicDoesNotDeadlockLibrary verifies that panicking
// inside OnPotentialDeadlock does not leave lo.mu permanently locked (issue #29).
func TestOnPotentialDeadlockPanicDoesNotDeadlockLibrary(t *testing.T) {
	defer restore()()
	Opts.DeadlockTimeout = 0

	var a, b, unrelated Mutex
	a.Lock()
	b.Lock()
	b.Unlock()
	a.Unlock()

	Opts.OnPotentialDeadlock = func() {
		panic("deadlock-callback-panic")
	}

	var recovered uint32
	func() {
		defer func() {
			if recover() != nil {
				atomic.StoreUint32(&recovered, 1)
			}
		}()
		b.Lock()
		defer b.Unlock()
		a.Lock()
		a.Unlock()
	}()

	if atomic.LoadUint32(&recovered) == 0 {
		t.Fatal("expected panic from OnPotentialDeadlock")
	}

	ok := make(chan struct{})
	go func() {
		unrelated.Lock()
		unrelated.Unlock()
		close(ok)
	}()

	select {
	case <-ok:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("lo.mu remained locked after panic in OnPotentialDeadlock")
	}
}

// TestOnPotentialDeadlockConcurrentMutationsSafe verifies that concurrent lock/unlock
// activity in other goroutines during OnPotentialDeadlock does not race on lo.cur
// or pooled stack buffers.
func TestOnPotentialDeadlockConcurrentMutationsSafe(t *testing.T) {
	defer restore()()
	Opts.DeadlockTimeout = 0

	var a, b Mutex
	a.Lock()
	b.Lock()
	b.Unlock()
	a.Unlock()

	var stop uint32
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var m Mutex
			for atomic.LoadUint32(&stop) == 0 {
				m.Lock()
				m.Unlock()
			}
		}()
	}

	var calls int32
	Opts.OnPotentialDeadlock = func() {
		atomic.AddInt32(&calls, 1)
		time.Sleep(10 * time.Millisecond)
	}

	for i := 0; i < 5; i++ {
		b.Lock()
		a.Lock()
		a.Unlock()
		b.Unlock()
	}

	atomic.StoreUint32(&stop, 1)
	wg.Wait()
	if got := atomic.LoadInt32(&calls); got < 5 {
		t.Fatalf("expected at least 5 callbacks, got %d", got)
	}
}

// TestRecursiveRLockFastPath verifies that the TryRLock fast path still detects
// recursive RLock calls on the same goroutine and releases the lock on callback panic.
func TestRecursiveRLockFastPath(t *testing.T) {
	defer restore()()
	Opts.DeadlockTimeout = 0

	var a RWMutex
	Opts.OnPotentialDeadlock = func() {
		panic("recursive-rlock")
	}

	var panicked uint32
	func() {
		defer func() {
			if recover() != nil {
				atomic.StoreUint32(&panicked, 1)
			}
		}()
		a.RLock()
		defer a.RUnlock()
		a.RLock()
		a.RUnlock()
	}()

	if atomic.LoadUint32(&panicked) == 0 {
		t.Fatal("expected panic on recursive RLock")
	}
	// Both RLock calls must be released after panic rollback so write Lock succeeds.
	a.Lock()
	a.Unlock()
}
