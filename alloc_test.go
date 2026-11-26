package deadlock

import (
	"sync"
	"testing"
)

func BenchmarkRegisterDeregister(b *testing.B) {
	for i := 0; i < b.N; i++ {
		id := dw.register(nil, nil, 0, Opts.DeadlockTimeout)
		dw.deregister(id)
	}
}

func BenchmarkLockUnlock(b *testing.B) {
	var mu Mutex
	for i := 0; i < b.N; i++ {
		mu.Lock()
		mu.Unlock()
	}
}

func BenchmarkNestedLockUnlock(b *testing.B) {
	var a, c Mutex
	for i := 0; i < b.N; i++ {
		a.Lock()
		c.Lock()
		c.Unlock()
		a.Unlock()
	}
}

func BenchmarkLockUnlockWithBackgroundHolders(b *testing.B) {
	// Simulates concurrent goroutines holding other mutexes while this goroutine locks an unrelated mutex.
	const n = 128
	holders := make([]Mutex, n)
	ready := make(chan struct{}, n)
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		idx := i
		go func() {
			defer wg.Done()
			holders[idx].Lock()
			ready <- struct{}{}
			<-release
			holders[idx].Unlock()
		}()
	}
	for i := 0; i < n; i++ {
		<-ready
	}
	defer func() {
		close(release)
		wg.Wait()
	}()
	var mu Mutex
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		mu.Unlock()
	}
}

func BenchmarkParallelDisjointLockUnlock(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		var mu Mutex
		for pb.Next() {
			mu.Lock()
			mu.Unlock()
		}
	})
}

func BenchmarkParallelNestedLockUnlock(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		var a, c Mutex
		for pb.Next() {
			a.Lock()
			c.Lock()
			c.Unlock()
			a.Unlock()
		}
	})
}
