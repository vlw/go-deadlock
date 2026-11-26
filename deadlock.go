package deadlock

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/petermattis/goid"
)

// TimerPoolMode controls timer pooling behavior
type TimerPoolMode int

const (
	// TimerPoolDefault automatically chooses based on build environment
	TimerPoolDefault TimerPoolMode = iota
	// TimerPoolEnabled always uses timer pooling for performance
	TimerPoolEnabled
	// TimerPoolDisabled disables timer pooling (required for testing/synctest)
	TimerPoolDisabled
)

// Opts control how deadlock detection behaves.
// Options are supposed to be set once at a startup (say, when parsing flags).
var Opts = struct {
	// Mutex/RWMutex would work exactly as their sync counterparts
	// -- almost no runtime penalty, no deadlock detection if Disable == true.
	Disable bool
	// Would disable lock order based deadlock detection if DisableLockOrderDetection == true.
	DisableLockOrderDetection bool
	// Waiting for a lock for longer than DeadlockTimeout is considered a deadlock.
	// Ignored if DeadlockTimeout <= 0.
	DeadlockTimeout time.Duration
	// OnPotentialDeadlock is called each time a potential deadlock is detected -- either based on
	// lock order or on lock wait time.
	OnPotentialDeadlock func()
	// Will keep MaxMapSize lock pairs (happens before // happens after) in the map.
	// The map resets once the threshold is reached.
	MaxMapSize int
	// Will dump stacktraces of all goroutines when inconsistent locking is detected.
	PrintAllCurrentGoroutines bool
	// Controls timer pooling behavior.
	// TimerPoolDefault: Automatically choose based on build environment
	// TimerPoolEnabled: Always use timer pooling
	// TimerPoolDisabled: Never use timer pooling
	TimerPool TimerPoolMode
	mu        *sync.Mutex // Protects the LogBuf.
	// Will print deadlock info to log buffer.
	LogBuf io.Writer
}{
	DeadlockTimeout: time.Second * 30,
	OnPotentialDeadlock: func() {
		os.Exit(2)
	},
	MaxMapSize: 1024 * 64,
	mu:         &sync.Mutex{},
	LogBuf:     os.Stderr,
}

// Cond is sync.Cond wrapper
type Cond struct {
	sync.Cond
}

// Locker is sync.Locker wrapper
type Locker struct {
	sync.Locker
}

// Once is sync.Once wrapper
type Once struct {
	sync.Once
}

// Pool is sync.Poll wrapper
type Pool struct {
	sync.Pool
}

// WaitGroup is sync.WaitGroup wrapper
type WaitGroup struct {
	sync.WaitGroup
}

// NewCond is a sync.NewCond wrapper
var NewCond = sync.NewCond

// A Mutex is a drop-in replacement for sync.Mutex.
// Performs deadlock detection unless disabled in Opts.
type Mutex struct {
	mu StandardMutex
}

// Lock locks the mutex.
// If the lock is already in use, the calling goroutine
// blocks until the mutex is available.
//
// Unless deadlock detection is disabled, logs potential deadlocks to Opts.LogBuf,
// calling Opts.OnPotentialDeadlock on each occasion.
func (m *Mutex) Lock() {
	if Opts.Disable {
		m.mu.Lock()
		return
	}
	if m.mu.TryLock() {
		if violations := lo.preAndPostLock(goid.Get(), unsafe.Pointer(m)); violations > 0 {
			handleViolations(violations, unsafe.Pointer(m), m.mu.Unlock)
		}
		return
	}
	lockSlow(m.mu.Lock, unsafe.Pointer(m))
}

// Unlock unlocks the mutex.
// It is a run-time error if m is not locked on entry to Unlock.
//
// A locked Mutex is not associated with a particular goroutine.
// It is allowed for one goroutine to lock a Mutex and then
// arrange for another goroutine to unlock it.
func (m *Mutex) Unlock() {
	if !Opts.Disable {
		lo.postUnlock(unsafe.Pointer(m))
	}
	m.mu.Unlock()
}

// An RWMutex is a drop-in replacement for sync.RWMutex.
// Performs deadlock detection unless disabled in Opts.
type RWMutex struct {
	mu StandardRWMutex
}

// Lock locks rw for writing.
// If the lock is already locked for reading or writing,
// Lock blocks until the lock is available.
// To ensure that the lock eventually becomes available,
// a blocked Lock call excludes new readers from acquiring
// the lock.
//
// Unless deadlock detection is disabled, logs potential deadlocks to Opts.LogBuf,
// calling Opts.OnPotentialDeadlock on each occasion.
func (m *RWMutex) Lock() {
	if Opts.Disable {
		m.mu.Lock()
		return
	}
	if m.mu.TryLock() {
		if violations := lo.preAndPostLock(goid.Get(), unsafe.Pointer(m)); violations > 0 {
			handleViolations(violations, unsafe.Pointer(m), m.mu.Unlock)
		}
		return
	}
	lockSlow(m.mu.Lock, unsafe.Pointer(m))
}

// Unlock unlocks the mutex for writing.  It is a run-time error if rw is
// not locked for writing on entry to Unlock.
//
// As with Mutexes, a locked RWMutex is not associated with a particular
// goroutine.  One goroutine may RLock (Lock) an RWMutex and then
// arrange for another goroutine to RUnlock (Unlock) it.
func (m *RWMutex) Unlock() {
	if !Opts.Disable {
		lo.postUnlock(unsafe.Pointer(m))
	}
	m.mu.Unlock()
}

// RLock locks the mutex for reading.
//
// Unless deadlock detection is disabled, logs potential deadlocks to Opts.LogBuf,
// calling Opts.OnPotentialDeadlock on each occasion.
func (m *RWMutex) RLock() {
	if Opts.Disable {
		m.mu.RLock()
		return
	}
	if m.mu.TryRLock() {
		if violations := lo.preAndPostLock(goid.Get(), unsafe.Pointer(m)); violations > 0 {
			handleViolations(violations, unsafe.Pointer(m), m.mu.RUnlock)
		}
		return
	}
	lockSlow(m.mu.RLock, unsafe.Pointer(m))
}

// RUnlock undoes a single RLock call;
// it does not affect other simultaneous readers.
// It is a run-time error if rw is not locked for reading
// on entry to RUnlock.
func (m *RWMutex) RUnlock() {
	if !Opts.Disable {
		lo.postUnlock(unsafe.Pointer(m))
	}
	m.mu.RUnlock()
}

// RLocker returns a Locker interface that implements
// the Lock and Unlock methods by calling RLock and RUnlock.
func (m *RWMutex) RLocker() sync.Locker {
	return m.mu.RLocker()
}

// handleViolations invokes Opts.OnPotentialDeadlock for each violation found
// after a successful TryLock/TryRLock fast path, rolling back tracking and the
// acquired lock if the callback panics.
//
//go:noinline
func handleViolations(violations int, ptr unsafe.Pointer, unlockFn func()) {
	panicked := true
	defer func() {
		if panicked {
			lo.postUnlock(ptr)
			unlockFn()
		}
	}()
	for range violations {
		Opts.OnPotentialDeadlock()
	}
	panicked = false
}

// lockSlow handles contended acquisitions where TryLock/TryRLock failed.
//
//go:noinline
func lockSlow(lockFn func(), ptr unsafe.Pointer) {
	gid := goid.Get()
	var stackBuf [stackBufSize]uintptr
	stack := stackBuf[:fpCallers(stackBuf[:])]

	// Cache timeout before preLock so all Opts reads complete before preLock
	// may call OnPotentialDeadlock. If preLock detects a problem (recursive
	// lock, order violation) the goroutine may block forever in lockFn below,
	// and reading Opts after preLock would race with any later Opts write.
	timeout := Opts.DeadlockTimeout
	lo.preLock(gid, stack, ptr)
	if timeout <= 0 {
		lockFn()
	} else {
		e := dw.register(stack, ptr, gid, timeout)
		lockFn()
		dw.deregister(e)
	}
	lo.postLock(gid, stack, ptr)
}

// pendingEntry tracks a goroutine that is waiting to acquire a lock. Entries are
// pooled to avoid per-lock heap allocations (goroutine stacks, channels, closures).
// The waiting stack is copied inline into stack so onDeadlockTimeout never
// aliases a caller-owned buffer.
//
// Timer safety invariants:
//   - checkFn is allocated once per entry and reused across pool cycles, so recycling
//     an entry does not allocate a new closure.
//   - The done flag synchronizes the callback with deregister: deregister sets done=1
//     before calling Stop(), and the callback checks done before acting. Because both
//     use atomic operations, the callback is guaranteed to observe done=1 if deregister
//     has already run, even if the runtime already scheduled the callback.
//   - An entry is only returned to the pool when timer.Stop() returns true, meaning
//     the timer was successfully cancelled and the callback will never run. This prevents
//     a recycled entry from being mutated by an in-flight callback.
//   - When Stop() returns false (callback already firing or queued), the entry is
//     intentionally leaked to GC. This only happens in the rare deadlock-timeout path.
type pendingEntry struct {
	stack    [stackBufSize]uintptr
	stackLen int
	ptr      unsafe.Pointer
	gid      int64
	done     int32 // atomic: 0=pending, 1=acquired
	timer    *time.Timer
	checkFn  func()
}

func newPendingEntry() *pendingEntry {
	e := &pendingEntry{}
	// Capture e by pointer so the closure is stable across pool reuse, no new
	// closure allocation when the entry is recycled.
	e.checkFn = func() {
		// If the lock was acquired (done=1), the entry may already be back in the
		// pool or being reused. Bail out unconditionally.
		if atomic.LoadInt32(&e.done) != 0 {
			return
		}
		onDeadlockTimeout(e)
	}
	return e
}

var pendingPool = sync.Pool{
	New: func() any {
		return newPendingEntry()
	},
}

type deadlockWatcher struct{}

var dw deadlockWatcher

func (w *deadlockWatcher) register(stack []uintptr, ptr unsafe.Pointer, gid int64, timeout time.Duration) *pendingEntry {
	var e *pendingEntry
	if shouldDisableTimerPool() {
		e = newPendingEntry()
	} else {
		e = pendingPool.Get().(*pendingEntry)
	}
	e.stackLen = copy(e.stack[:], stack)
	e.ptr = ptr
	e.gid = gid
	atomic.StoreInt32(&e.done, 0)
	if e.timer == nil {
		// First use (freshly allocated entry): create the AfterFunc timer.
		// AfterFunc avoids the channel-drain problems of channel-based timers,
		// which are especially problematic under testing/synctest.
		e.timer = time.AfterFunc(timeout, e.checkFn)
	} else {
		// Reused from pool: the timer was previously Stop()'d successfully
		// (guaranteed by deregister), so Reset is safe here.
		e.timer.Reset(timeout)
	}
	return e
}

// deregister marks the lock as acquired and cancels the deadlock timer.
// Must be called exactly once per register call. The entry pointer is
// stack-local in lock(), so concurrent or duplicate calls cannot occur.
func (w *deadlockWatcher) deregister(e *pendingEntry) {
	// Mark done BEFORE stopping the timer. The callback checks done with an
	// atomic load, so even if the timer fires concurrently, the callback will
	// see done=1 and return without acting.
	atomic.StoreInt32(&e.done, 1)
	stopped := e.timer.Stop()
	// Only recycle the entry if Stop() confirmed the callback won't run.
	// If Stop() returned false the callback is already executing or queued;
	// recycling would race with the callback reading entry fields.
	if stopped && !shouldDisableTimerPool() {
		e.stackLen = 0
		e.ptr = nil
		e.gid = 0
		pendingPool.Put(e)
	}
}

type holderSnapshot struct {
	gid   int64
	ptr   unsafe.Pointer
	stack []uintptr
}

func onDeadlockTimeout(e *pendingEntry) {
	holders := lo.snapshotHolders(e.ptr)
	if len(holders) == 0 {
		// Lock appears unheld (transient state, holder may have just released).
		// Reschedule if the waiter is still pending. Note: this creates a new timer
		// (e.timer is not updated), so if deregister runs later it will Stop() the
		// original (already-fired) timer, get false, and skip pooling. The new timer's
		// callback will then observe done=1 and no-op. This is safe but means the
		// entry won't be recycled, acceptable since this is the rare timeout path.
		if atomic.LoadInt32(&e.done) == 0 {
			time.AfterFunc(Opts.DeadlockTimeout, e.checkFn)
		}
		return
	}
	Opts.mu.Lock()
	fmt.Fprintln(Opts.LogBuf, header)
	for _, prev := range holders {
		fmt.Fprintln(Opts.LogBuf, "Previous place where the lock was grabbed")
		fmt.Fprintf(Opts.LogBuf, "goroutine %v lock %p\n", prev.gid, e.ptr)
		printStack(Opts.LogBuf, prev.stack)
	}
	fmt.Fprintln(Opts.LogBuf, "Have been trying to lock it again for more than", Opts.DeadlockTimeout)
	fmt.Fprintf(Opts.LogBuf, "goroutine %v lock %p\n", e.gid, e.ptr)
	printStack(Opts.LogBuf, e.stack[:e.stackLen])
	stacks := stacks()
	grs := bytes.Split(stacks, []byte("\n\n"))
	for _, prev := range holders {
		for _, g := range grs {
			if goid.ExtractGID(g) == prev.gid {
				fmt.Fprintln(Opts.LogBuf, "Here is what goroutine", prev.gid, "doing now")
				Opts.LogBuf.Write(g)
				fmt.Fprintln(Opts.LogBuf)
			}
		}
	}
	lo.other(e.ptr)
	if Opts.PrintAllCurrentGoroutines {
		fmt.Fprintln(Opts.LogBuf, "All current goroutines:")
		Opts.LogBuf.Write(stacks)
	}
	fmt.Fprintln(Opts.LogBuf)
	if buf, ok := Opts.LogBuf.(*bufio.Writer); ok {
		buf.Flush()
	}
	Opts.mu.Unlock()
	Opts.OnPotentialDeadlock()
}

const (
	gidShardCount  = 256
	gidShardMask   = gidShardCount - 1
	gidInlineSlots = 4
	gidFullMask    = (1 << gidInlineSlots) - 1
)

// heldSlot stores a single active lock acquisition owned by gid.
// Stack frames are kept inline so lock/unlock requires no pools or heap allocations.
type heldSlot struct {
	gid      int64
	ptr      unsafe.Pointer
	stackLen uint8
	stack    [stackBufSize]uintptr
}

type gidShardBody struct {
	mu       sync.Mutex
	mask     uint8 // bit i is set iff inline[i] is occupied
	inline   [gidInlineSlots]heldSlot
	overflow []heldSlot
}

type gidShard struct {
	gidShardBody
	_ [(cacheLinePad - unsafe.Sizeof(gidShardBody{})%cacheLinePad) % cacheLinePad]byte
}

// lockOrder tracks live lock ownership in 256 goroutine-ID shards and historical
// lock-ordering edges in a lock-free sharded table.
//
// Lock ordering hierarchy (strict DAG, never violated):
//   - gidShard.mu -> orderShard.mu (one gidShard.mu at a time)
//   - Opts.mu -> gidShard.mu (one gidShard.mu at a time, inside other/timeout reporting)
//
// No function ever acquires two gidShard.mu locks simultaneously, nor acquires
// Opts.mu while holding any gidShard.mu.
type lockOrder struct {
	shards [gidShardCount]gidShard
	orders orderStore
}

var lo lockOrder

func hashGid(gid int64) uint64 {
	return (uint64(gid) * 0x9e3779b97f4a7c15) >> 56
}

type orderViolation struct {
	prevBefore []uintptr
	prevAfter  []uintptr
	curBefore  []uintptr
	curAfter   []uintptr
}

// preAndPostLock checks lock ordering and registers the acquisition in a single
// shard lock acquisition for the TryLock/TryRLock fast path.
func (l *lockOrder) preAndPostLock(gid int64, ptr unsafe.Pointer) int {
	sh := &l.shards[hashGid(gid)]
	sh.mu.Lock()

	// Fast path: shard is completely empty (common single-lock case).
	if sh.mask == 0 && len(sh.overflow) == 0 {
		slot := &sh.inline[0]
		slot.gid = gid
		slot.ptr = ptr
		slot.stackLen = uint8(fpCallers(slot.stack[:]))
		sh.mask = 1
		sh.mu.Unlock()
		return 0
	}

	// Reserve a slot for the new acquisition and capture its stack directly into it.
	oldMask := sh.mask
	oldOverflowLen := len(sh.overflow)
	var newSlot *heldSlot
	var newInlineBit uint8
	if oldMask != gidFullMask {
		idx := bits.TrailingZeros8(^oldMask)
		newInlineBit = uint8(1) << idx
		newSlot = &sh.inline[idx]
	} else {
		sh.overflow = append(sh.overflow, heldSlot{})
		newSlot = &sh.overflow[oldOverflowLen]
	}
	newSlot.gid = gid
	newSlot.ptr = ptr
	newSlot.stackLen = uint8(fpCallers(newSlot.stack[:]))
	curStack := newSlot.stack[:newSlot.stackLen]

	var recursivePrev []uintptr
	var recursiveCur []uintptr
	var violations []orderViolation

	if !Opts.DisableLockOrderDetection {
		recursivePrev, recursiveCur, violations = l.checkShardLocked(sh, oldMask, oldOverflowLen, gid, curStack, ptr)
	}

	if newInlineBit != 0 {
		sh.mask = oldMask | newInlineBit
	}
	sh.mu.Unlock()

	return l.reportViolations(gid, ptr, recursivePrev, recursiveCur, violations)
}

// checkShardLocked checks the calling goroutine's previously held locks inside sh
// (restricted to oldMask and sh.overflow[:oldOverflowLen]) against a new acquisition
// of ptr. Any violation stacks are copied out so reporting runs after sh.mu is released.
func (l *lockOrder) checkShardLocked(
	sh *gidShard,
	oldMask uint8,
	oldOverflowLen int,
	gid int64,
	curStack []uintptr,
	ptr unsafe.Pointer,
) (recursivePrev, recursiveCur []uintptr, violations []orderViolation) {
	for m := oldMask; m != 0; m &= m - 1 {
		h := &sh.inline[bits.TrailingZeros8(m)]
		if h.gid != gid {
			continue
		}
		if h.ptr == ptr {
			return copyStack(h.stack[:h.stackLen]), copyStack(curStack), nil
		}
		if before, after, violated := l.orders.checkAndRecord(h.ptr, ptr, h.stack[:h.stackLen], curStack); violated {
			violations = append(violations, orderViolation{
				prevBefore: before,
				prevAfter:  after,
				curBefore:  copyStack(h.stack[:h.stackLen]),
				curAfter:   copyStack(curStack),
			})
		}
	}
	for i := range oldOverflowLen {
		h := &sh.overflow[i]
		if h.gid != gid {
			continue
		}
		if h.ptr == ptr {
			return copyStack(h.stack[:h.stackLen]), copyStack(curStack), nil
		}
		if before, after, violated := l.orders.checkAndRecord(h.ptr, ptr, h.stack[:h.stackLen], curStack); violated {
			violations = append(violations, orderViolation{
				prevBefore: before,
				prevAfter:  after,
				curBefore:  copyStack(h.stack[:h.stackLen]),
				curAfter:   copyStack(curStack),
			})
		}
	}
	return nil, nil, violations
}

// reportViolations logs any detected recursive locking or lock-order violations
// under Opts.mu (with no gidShard.mu held) and returns the violation count.
func (l *lockOrder) reportViolations(
	gid int64,
	ptr unsafe.Pointer,
	recursivePrev, recursiveCur []uintptr,
	violations []orderViolation,
) int {
	if recursivePrev != nil {
		Opts.mu.Lock()
		fmt.Fprintln(Opts.LogBuf, header, "Recursive locking:")
		fmt.Fprintf(Opts.LogBuf, "current goroutine %d lock %p\n", gid, ptr)
		printStack(Opts.LogBuf, recursiveCur)
		fmt.Fprintln(Opts.LogBuf, "Previous place where the lock was grabbed (same goroutine)")
		printStack(Opts.LogBuf, recursivePrev)
		l.other(ptr)
		if buf, ok := Opts.LogBuf.(*bufio.Writer); ok {
			buf.Flush()
		}
		Opts.mu.Unlock()
		return 1
	}
	if len(violations) == 0 {
		return 0
	}
	for _, v := range violations {
		Opts.mu.Lock()
		fmt.Fprintln(Opts.LogBuf, header, "Inconsistent locking. saw this ordering in one goroutine:")
		fmt.Fprintln(Opts.LogBuf, "happened before")
		printStack(Opts.LogBuf, v.prevBefore)
		fmt.Fprintln(Opts.LogBuf, "happened after")
		printStack(Opts.LogBuf, v.prevAfter)
		fmt.Fprintln(Opts.LogBuf, "in another goroutine: happened before")
		printStack(Opts.LogBuf, v.curBefore)
		fmt.Fprintln(Opts.LogBuf, "happened after")
		printStack(Opts.LogBuf, v.curAfter)
		l.other(ptr)
		fmt.Fprintln(Opts.LogBuf)
		if buf, ok := Opts.LogBuf.(*bufio.Writer); ok {
			buf.Flush()
		}
		Opts.mu.Unlock()
	}
	return len(violations)
}

func (l *lockOrder) preLock(gid int64, stack []uintptr, ptr unsafe.Pointer) {
	if Opts.DisableLockOrderDetection {
		return
	}
	sh := &l.shards[hashGid(gid)]
	sh.mu.Lock()
	if sh.mask == 0 && len(sh.overflow) == 0 {
		sh.mu.Unlock()
		return
	}
	recursivePrev, recursiveCur, violations := l.checkShardLocked(sh, sh.mask, len(sh.overflow), gid, stack, ptr)
	sh.mu.Unlock()

	n := l.reportViolations(gid, ptr, recursivePrev, recursiveCur, violations)
	for range n {
		Opts.OnPotentialDeadlock()
	}
}

func (l *lockOrder) postLock(gid int64, stack []uintptr, ptr unsafe.Pointer) {
	sh := &l.shards[hashGid(gid)]
	sh.mu.Lock()
	if sh.mask != gidFullMask {
		idx := bits.TrailingZeros8(^sh.mask)
		slot := &sh.inline[idx]
		slot.gid = gid
		slot.ptr = ptr
		slot.stackLen = uint8(copy(slot.stack[:], stack))
		sh.mask |= uint8(1) << idx
	} else {
		sh.overflow = append(sh.overflow, heldSlot{})
		slot := &sh.overflow[len(sh.overflow)-1]
		slot.gid = gid
		slot.ptr = ptr
		slot.stackLen = uint8(copy(slot.stack[:], stack))
	}
	sh.mu.Unlock()
}

func (l *lockOrder) postUnlock(ptr unsafe.Pointer) {
	gid := goid.Get()
	ownIdx := hashGid(gid)
	sh := &l.shards[ownIdx]
	sh.mu.Lock()

	// Fast path: single active slot owned by the unlocking goroutine.
	if sh.mask == 1 && sh.inline[0].ptr == ptr && sh.inline[0].gid == gid {
		sh.inline[0].ptr = nil
		sh.mask = 0
		sh.mu.Unlock()
		return
	}

	// Search this goroutine's entries in its own shard first.
	crossInlineIdx := -1
	crossOverflowIdx := -1
	for m := sh.mask; m != 0; m &= m - 1 {
		i := bits.TrailingZeros8(m)
		if sh.inline[i].ptr == ptr {
			if sh.inline[i].gid == gid {
				sh.inline[i].ptr = nil
				sh.mask &^= uint8(1) << i
				sh.mu.Unlock()
				return
			}
			if crossInlineIdx < 0 {
				crossInlineIdx = i
			}
		}
	}
	for i := len(sh.overflow) - 1; i >= 0; i-- {
		if sh.overflow[i].ptr == ptr {
			if sh.overflow[i].gid == gid {
				removeOverflowSlot(sh, i)
				sh.mu.Unlock()
				return
			}
			if crossOverflowIdx < 0 {
				crossOverflowIdx = i
			}
		}
	}

	// Cross-goroutine unlock: Go permits one goroutine to Lock and another to
	// Unlock. If another goroutine hashed to the same shard holds ptr, remove it
	// directly; otherwise scan the remaining shards one by one.
	if crossInlineIdx >= 0 {
		sh.inline[crossInlineIdx].ptr = nil
		sh.mask &^= uint8(1) << crossInlineIdx
		sh.mu.Unlock()
		return
	}
	if crossOverflowIdx >= 0 {
		removeOverflowSlot(sh, crossOverflowIdx)
		sh.mu.Unlock()
		return
	}
	sh.mu.Unlock()

	for i := range gidShardCount {
		if uint64(i) == ownIdx {
			continue
		}
		other := &l.shards[i]
		other.mu.Lock()
		if removeAnyByPtrLocked(other, ptr) {
			other.mu.Unlock()
			return
		}
		other.mu.Unlock()
	}
}

func removeOverflowSlot(sh *gidShard, idx int) {
	last := len(sh.overflow) - 1
	sh.overflow[idx] = sh.overflow[last]
	sh.overflow[last] = heldSlot{}
	sh.overflow = sh.overflow[:last]
}

func removeAnyByPtrLocked(sh *gidShard, ptr unsafe.Pointer) bool {
	for m := sh.mask; m != 0; m &= m - 1 {
		i := bits.TrailingZeros8(m)
		if sh.inline[i].ptr == ptr {
			sh.inline[i].ptr = nil
			sh.mask &^= uint8(1) << i
			return true
		}
	}
	for i := len(sh.overflow) - 1; i >= 0; i-- {
		if sh.overflow[i].ptr == ptr {
			removeOverflowSlot(sh, i)
			return true
		}
	}
	return false
}

// snapshotHolders scans all shards sequentially (never holding two shard locks
// at once) and returns copies of every active holder entry matching ptr.
func (l *lockOrder) snapshotHolders(ptr unsafe.Pointer) []holderSnapshot {
	var holders []holderSnapshot
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for m := sh.mask; m != 0; m &= m - 1 {
			slot := &sh.inline[bits.TrailingZeros8(m)]
			if slot.ptr == ptr {
				holders = append(holders, holderSnapshot{
					gid:   slot.gid,
					ptr:   slot.ptr,
					stack: copyStack(slot.stack[:slot.stackLen]),
				})
			}
		}
		for j := range sh.overflow {
			slot := &sh.overflow[j]
			if slot.ptr == ptr {
				holders = append(holders, holderSnapshot{
					gid:   slot.gid,
					ptr:   slot.ptr,
					stack: copyStack(slot.stack[:slot.stackLen]),
				})
			}
		}
		sh.mu.Unlock()
	}
	return holders
}

// Under Opts.mu locked, with no gidShard.mu held.
func (l *lockOrder) other(ptr unsafe.Pointer) {
	var others []holderSnapshot
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for m := sh.mask; m != 0; m &= m - 1 {
			slot := &sh.inline[bits.TrailingZeros8(m)]
			if slot.ptr != ptr {
				others = append(others, holderSnapshot{
					gid:   slot.gid,
					ptr:   slot.ptr,
					stack: copyStack(slot.stack[:slot.stackLen]),
				})
			}
		}
		for j := range sh.overflow {
			slot := &sh.overflow[j]
			if slot.ptr != ptr {
				others = append(others, holderSnapshot{
					gid:   slot.gid,
					ptr:   slot.ptr,
					stack: copyStack(slot.stack[:slot.stackLen]),
				})
			}
		}
		sh.mu.Unlock()
	}
	if len(others) == 0 {
		return
	}
	fmt.Fprintln(Opts.LogBuf, "Other goroutines holding locks:")
	for _, pp := range others {
		fmt.Fprintf(Opts.LogBuf, "goroutine %v lock %p\n", pp.gid, pp.ptr)
		printStack(Opts.LogBuf, pp.stack)
	}
	fmt.Fprintln(Opts.LogBuf)
}

const header = "POTENTIAL DEADLOCK:"
