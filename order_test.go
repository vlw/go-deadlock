package deadlock

import (
	"runtime"
	"sync"
	"testing"
	"unsafe"
)

func TestMutexSizeMatchesStdlib(t *testing.T) {
	if got, want := unsafe.Sizeof(Mutex{}), unsafe.Sizeof(StandardMutex{}); got != want {
		t.Fatalf("sizeof(Mutex) = %d, want %d", got, want)
	}
	if got, want := unsafe.Sizeof(RWMutex{}), unsafe.Sizeof(StandardRWMutex{}); got != want {
		t.Fatalf("sizeof(RWMutex) = %d, want %d", got, want)
	}
}

func TestOrderStoreConcurrentCheckAndRecord(t *testing.T) {
	var os orderStore
	var a, b Mutex
	s1 := []uintptr{1, 2}
	s2 := []uintptr{3, 4}

	if _, _, violated := os.checkAndRecord(unsafe.Pointer(&a), unsafe.Pointer(&b), s1, s2); violated {
		t.Fatal("first a->b observation should not report a violation")
	}

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if _, _, v := os.checkAndRecord(unsafe.Pointer(&a), unsafe.Pointer(&b), s1, s2); v {
					t.Error("consistent a->b observation reported a violation")
					return
				}
			}
		}()
	}
	wg.Wait()

	// Reverse order b -> a must report conflict with original a -> b stacks.
	cb, ca, violated := os.checkAndRecord(unsafe.Pointer(&b), unsafe.Pointer(&a), s2, s1)
	if !violated || len(cb) != 2 || len(ca) != 2 {
		t.Fatalf("expected b->a violation with original stacks, got violated=%v cb=%v ca=%v", violated, cb, ca)
	}

	// Once both directions are recorded, subsequent a -> b also reports conflict.
	if _, _, violated := os.checkAndRecord(unsafe.Pointer(&a), unsafe.Pointer(&b), s1, s2); !violated {
		t.Fatal("expected a->b violation after reverse direction was recorded")
	}
}

func TestOrderStorePrunesGarbageCollectedEntries(t *testing.T) {
	var os orderStore
	var anchor Mutex
	s1 := []uintptr{1}
	s2 := []uintptr{2}

	for range 200 {
		m := new(Mutex)
		os.checkAndRecord(unsafe.Pointer(&anchor), unsafe.Pointer(m), s1, s2)
	}
	runtime.GC()
	runtime.GC()

	// Trigger rehash/pruning across shards by inserting a few more pairs.
	for range 64 {
		m := new(Mutex)
		os.checkAndRecord(unsafe.Pointer(&anchor), unsafe.Pointer(m), s1, s2)
	}
	runtime.KeepAlive(&anchor)
}

func TestOrderStoreMaxMapSizeReset(t *testing.T) {
	defer restore()()
	Opts.MaxMapSize = 16

	var os orderStore
	var anchor Mutex
	s1 := []uintptr{1}
	s2 := []uintptr{2}
	ms := make([]Mutex, 40)
	for i := range ms {
		os.checkAndRecord(unsafe.Pointer(&anchor), unsafe.Pointer(&ms[i]), s1, s2)
	}
	if got := os.total.Load(); got > int64(Opts.MaxMapSize)+1 {
		t.Fatalf("expected orderStore total to stay bounded near MaxMapSize=%d, got %d", Opts.MaxMapSize, got)
	}
	runtime.KeepAlive(ms)
	runtime.KeepAlive(&anchor)
}
