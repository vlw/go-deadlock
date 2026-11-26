package deadlock

import (
	"sync"
	"sync/atomic"
	"unsafe"
	"weak"
)

const (
	orderShardCount = 64
	orderShardMask  = orderShardCount - 1
	cacheLinePad    = 128
	orderInitSlots  = 8
)

// orderEntry records observed lock acquisition orders between a canonical pair
// of lock addresses (a < b). Weak pointers guard against false positives when a
// mutex is garbage-collected and another mutex is later allocated at the same
// address, without retaining mutexes on the heap.
//
// An orderEntry is strictly immutable once published into an orderTable slot.
// Recording the reverse direction or replacing a GC'd entry publishes a new
// *orderEntry atomically into the slot.
type orderEntry struct {
	a, b     uintptr
	aWeak    weak.Pointer[byte]
	bWeak    weak.Pointer[byte]
	abBefore []uintptr // non-nil when a -> b has been observed
	abAfter  []uintptr
	baBefore []uintptr // non-nil when b -> a has been observed
	baAfter  []uintptr
}

// orderTable is a power-of-two open-addressed table of immutable *orderEntry
// pointers. Steady-state lookups require zero locks, zero atomic writes, and
// zero weak.Make calls.
type orderTable struct {
	mask  uint64
	slots []atomic.Pointer[orderEntry]
}

type orderShardBody struct {
	table atomic.Pointer[orderTable]
	mu    sync.Mutex
	count int // occupied slots in current table, protected by mu
}

type orderShard struct {
	orderShardBody
	_ [(cacheLinePad - unsafe.Sizeof(orderShardBody{})%cacheLinePad) % cacheLinePad]byte
}

type orderStore struct {
	shards [orderShardCount]orderShard
	total  atomic.Int64
}

func hashPair(a, b uintptr) uint64 {
	x := (uint64(a) >> 3) * 0x9e3779b97f4a7c15
	x ^= (uint64(b) >> 3) * 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (os *orderStore) reset() {
	for i := range os.shards {
		sh := &os.shards[i]
		sh.mu.Lock()
		if sh.count > 0 {
			os.total.Add(-int64(sh.count))
			sh.count = 0
		}
		sh.table.Store(nil)
		sh.mu.Unlock()
	}
}

// checkAndRecord checks whether acquiring afterPtr while holding beforePtr
// contradicts a previously observed ordering, and records (beforePtr -> afterPtr)
// if not yet present.
//
// In the steady state (order already known), this runs completely lock-free
// with a single atomic table probe and zero heap allocations.
func (os *orderStore) checkAndRecord(beforePtr, afterPtr unsafe.Pointer, beforeStack, afterStack []uintptr) (conflictBefore, conflictAfter []uintptr, violated bool) {
	bp := uintptr(beforePtr)
	ap := uintptr(afterPtr)

	var a, b uintptr
	var pa, pb *byte
	var aBeforeB bool
	if bp < ap {
		a, b = bp, ap
		pa, pb = (*byte)(beforePtr), (*byte)(afterPtr)
		aBeforeB = true
	} else {
		a, b = ap, bp
		pa, pb = (*byte)(afterPtr), (*byte)(beforePtr)
		aBeforeB = false
	}

	h := hashPair(a, b)
	sh := &os.shards[h&orderShardMask]
	probeStart := h >> 6

	if t := sh.table.Load(); t != nil {
		idx := probeStart & t.mask
		for {
			e := t.slots[idx].Load()
			if e == nil {
				break
			}
			if e.a == a && e.b == b {
				if e.aWeak.Value() == pa && e.bWeak.Value() == pb {
					if aBeforeB {
						if e.baBefore != nil {
							if e.abBefore != nil {
								return e.baBefore, e.baAfter, true
							}
							// Conflict found and a -> b is not recorded yet; record via slow path.
							break
						}
						if e.abBefore != nil {
							return nil, nil, false
						}
					} else {
						if e.abBefore != nil {
							if e.baBefore != nil {
								return e.abBefore, e.abAfter, true
							}
							break
						}
						if e.baBefore != nil {
							return nil, nil, false
						}
					}
				}
				// Entry is stale (one of the mutexes was GC'd); replace via slow path.
				break
			}
			idx = (idx + 1) & t.mask
		}
	}

	return os.checkAndRecordSlow(sh, probeStart, a, b, pa, pb, aBeforeB, beforeStack, afterStack)
}

func (os *orderStore) checkAndRecordSlow(
	sh *orderShard,
	probeStart uint64,
	a, b uintptr,
	pa, pb *byte,
	aBeforeB bool,
	beforeStack, afterStack []uintptr,
) ([]uintptr, []uintptr, bool) {
	if maxSize := Opts.MaxMapSize; maxSize > 0 && os.total.Load() >= int64(maxSize) {
		os.reset()
	}

	sh.mu.Lock()
	defer sh.mu.Unlock()

	t := sh.table.Load()
	if t == nil {
		t = &orderTable{
			mask:  orderInitSlots - 1,
			slots: make([]atomic.Pointer[orderEntry], orderInitSlots),
		}
		sh.table.Store(t)
	}

	idx := probeStart & t.mask
	for {
		e := t.slots[idx].Load()
		if e == nil {
			// Need to insert a new slot; grow or prune dead weak entries if >= 50% full.
			if (sh.count+1)*2 > len(t.slots) {
				t = os.rehashLocked(sh, t)
				return os.insertFreshLocked(sh, t, probeStart, a, b, pa, pb, aBeforeB, beforeStack, afterStack)
			}
			t.slots[idx].Store(newOrderEntry(a, b, pa, pb, aBeforeB, beforeStack, afterStack))
			sh.count++
			os.total.Add(1)
			return nil, nil, false
		}
		if e.a == a && e.b == b {
			if e.aWeak.Value() == pa && e.bWeak.Value() == pb {
				var conflictBefore, conflictAfter []uintptr
				var violated bool
				if aBeforeB {
					if e.baBefore != nil {
						conflictBefore, conflictAfter, violated = e.baBefore, e.baAfter, true
					}
					if e.abBefore == nil {
						updated := *e
						updated.abBefore = copyStack(beforeStack)
						updated.abAfter = copyStack(afterStack)
						t.slots[idx].Store(&updated)
					}
				} else {
					if e.abBefore != nil {
						conflictBefore, conflictAfter, violated = e.abBefore, e.abAfter, true
					}
					if e.baBefore == nil {
						updated := *e
						updated.baBefore = copyStack(beforeStack)
						updated.baAfter = copyStack(afterStack)
						t.slots[idx].Store(&updated)
					}
				}
				return conflictBefore, conflictAfter, violated
			}
			// Replace stale GC'd entry in place without increasing slot count.
			t.slots[idx].Store(newOrderEntry(a, b, pa, pb, aBeforeB, beforeStack, afterStack))
			return nil, nil, false
		}
		idx = (idx + 1) & t.mask
	}
}

func (os *orderStore) insertFreshLocked(
	sh *orderShard,
	t *orderTable,
	probeStart uint64,
	a, b uintptr,
	pa, pb *byte,
	aBeforeB bool,
	beforeStack, afterStack []uintptr,
) ([]uintptr, []uintptr, bool) {
	idx := probeStart & t.mask
	for {
		if t.slots[idx].Load() == nil {
			t.slots[idx].Store(newOrderEntry(a, b, pa, pb, aBeforeB, beforeStack, afterStack))
			sh.count++
			os.total.Add(1)
			return nil, nil, false
		}
		idx = (idx + 1) & t.mask
	}
}

// rehashLocked compacts live entries from old into a fresh orderTable, pruning
// entries whose mutexes have been garbage-collected and doubling capacity only
// when live occupancy still exceeds 25%.
func (os *orderStore) rehashLocked(sh *orderShard, old *orderTable) *orderTable {
	live := make([]*orderEntry, 0, sh.count)
	for i := range old.slots {
		e := old.slots[i].Load()
		if e == nil {
			continue
		}
		if e.aWeak.Value() == nil || e.bWeak.Value() == nil {
			continue
		}
		live = append(live, e)
	}

	newSize := len(old.slots)
	if len(live)*4 >= newSize {
		newSize *= 2
	}
	nt := &orderTable{
		mask:  uint64(newSize - 1),
		slots: make([]atomic.Pointer[orderEntry], newSize),
	}
	for _, e := range live {
		idx := (hashPair(e.a, e.b) >> 6) & nt.mask
		for nt.slots[idx].Load() != nil {
			idx = (idx + 1) & nt.mask
		}
		nt.slots[idx].Store(e)
	}
	delta := int64(len(live) - sh.count)
	sh.count = len(live)
	if delta != 0 {
		os.total.Add(delta)
	}
	sh.table.Store(nt)
	return nt
}

func newOrderEntry(a, b uintptr, pa, pb *byte, aBeforeB bool, beforeStack, afterStack []uintptr) *orderEntry {
	e := &orderEntry{
		a:     a,
		b:     b,
		aWeak: weak.Make(pa),
		bWeak: weak.Make(pb),
	}
	if aBeforeB {
		e.abBefore = copyStack(beforeStack)
		e.abAfter = copyStack(afterStack)
	} else {
		e.baBefore = copyStack(beforeStack)
		e.baAfter = copyStack(afterStack)
	}
	return e
}
