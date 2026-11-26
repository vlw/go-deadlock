# Online deadlock detection in go (golang). [![Try it online](https://img.shields.io/badge/try%20it-online-blue.svg)](https://wandbox.org/permlink/hJc6QCZowxbNm9WW) [![Docs](https://godoc.org/github.com/sasha-s/go-deadlock?status.svg)](https://godoc.org/github.com/sasha-s/go-deadlock) [![codecov](https://codecov.io/gh/sasha-s/go-deadlock/branch/main/graph/badge.svg)](https://codecov.io/gh/sasha-s/go-deadlock) [![version](https://badge.fury.io/gh/sasha-s%2Fgo-deadlock.svg)](https://github.com/sasha-s/go-deadlock/releases) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> **Performance & Stability Fork (`github.com/vlw/go-deadlock`)**
>
> This is an actively maintained fork of [`sasha-s/go-deadlock`](https://github.com/sasha-s/go-deadlock) focused on **production performance, stability, and minimal CPU, memory, and allocation overhead** without compromising any deadlock-detection capabilities:
> - **Zero allocations (`0 B/op, 0 allocs/op`)** on uncontended lock/unlock and steady-state nested lock-order checks.
> - **Zero per-mutex memory overhead**: `sizeof(deadlock.Mutex) == sizeof(sync.Mutex)` (`8 B`) and `sizeof(deadlock.RWMutex) == sizeof(sync.RWMutex)` (`24 B`).
> - **High multi-core scalability**: 256 cache-line-padded (128-byte) goroutine-ID shards with inline slots and a 64-shard lock-free canonical lock-order table (no global lock on the fast path).
> - **Fast stack capture**: frame-pointer unwinding on `amd64`/`arm64` with lazy `runtime.CallersFrames` expansion on deadlock reports (automatic `runtime.Callers` fallback on all other architectures and with `-tags deadlock_no_fpunwind`).
> - **Stability fixes**: safe `OnPotentialDeadlock` callback execution outside internal locks (fixes deadlocks on slow/panicking callbacks), safe `TryLock`/`TryRLock` failure handling, and full GC compatibility via `weak.Pointer`.
> - **Requires Go 1.27+**.
>
> To use this fork as a drop-in replacement without changing imports (automatically resolves to the latest release):
> ```sh
> go mod edit -replace github.com/sasha-s/go-deadlock=github.com/vlw/go-deadlock@latest
> go mod tidy
> ```

### Benchmarks

Measured with Go `1.27.4` on `linux/amd64` (AMD Ryzen 9 9950X3D, `GOMAXPROCS=32`, median of 3 runs, `-benchtime=500ms`):

| Benchmark | Upstream `sasha-s/go-deadlock` (`222d9c9`) | This fork `vlw/go-deadlock` (`v1.0.0`) | Stdlib `sync.Mutex` (Go 1.27.4) | Speedup vs Upstream |
|---|---:|---:|---:|---:|
| `Lock`/`Unlock` (1T) | `390.3 ns/op` · `24 B/op` · `1 allocs/op` | `27.87 ns/op` · `0 B/op` · `0 allocs/op` | `7.20 ns/op` · `0 B/op` · `0 allocs/op` | **14.0×** |
| Nested 2 locks (1T) | `959.1 ns/op` · `144 B/op` · `4 allocs/op` | `67.17 ns/op` · `0 B/op` · `0 allocs/op` | `14.22 ns/op` · `0 B/op` · `0 allocs/op` | **14.3×** |
| `Lock`/`Unlock` with 128 background holders (1T) | `1217 ns/op` · `24 B/op` · `1 allocs/op` | `27.91 ns/op` · `0 B/op` · `0 allocs/op` | `7.19 ns/op` · `0 B/op` · `0 allocs/op` | **43.6×** |
| Parallel disjoint `Lock`/`Unlock` (32T) | `624.6 ns/op` · `24 B/op` · `1 allocs/op` | `1.07 ns/op` · `0 B/op` · `0 allocs/op` | `0.26 ns/op` · `0 B/op` · `0 allocs/op` | **~584×** |
| Parallel nested 2 locks (32T) | `1161 ns/op` · `112 B/op` · `4 allocs/op` | `12.74 ns/op` *(4.76–13.87)* · `0 B/op` · `0 allocs/op` | `0.53 ns/op` · `0 B/op` · `0 allocs/op` | **~91×** |

## Why
Deadlocks happen and are painful to debug.

## What
go-deadlock provides (RW)Mutex drop-in replacements for sync.(RW)Mutex.
It would not work if you create a spaghetti of channels.
Mutexes only.

## Installation
```sh
go get github.com/sasha-s/go-deadlock/...
```

## Usage
```go
import "github.com/sasha-s/go-deadlock"
var mu deadlock.Mutex
// Use normally, it works exactly like sync.Mutex does.
mu.Lock()

defer mu.Unlock()
// Or
var rw deadlock.RWMutex
rw.RLock()
defer rw.RUnlock()
```

### Deadlocks
One of the most common sources of deadlocks is inconsistent lock ordering:
say, you have two mutexes A and B, and in some goroutines you have
```go
A.Lock() // defer A.Unlock() or similar.
...
B.Lock() // defer B.Unlock() or similar.
```
And in another goroutine the order of locks is reversed:
```go
B.Lock() // defer B.Unlock() or similar.
...
A.Lock() // defer A.Unlock() or similar.
```

Another common sources of deadlocks is duplicate take a lock in a goroutine:
```
A.Rlock() or lock()

A.lock() or A.RLock()
```

This does not guarantee a deadlock (maybe the goroutines above can never be running at the same time), but it usually a design flaw at least.

go-deadlock can detect such cases (unless you cross goroutine boundary - say lock A, then spawn a goroutine, block until it is singals, and lock B inside of the goroutine), even if the deadlock itself happens very infrequently and is painful to reproduce!

Each time go-deadlock sees a lock attempt for lock B, it records the order A before B, for each lock that is currently being held in the same goroutine, and it prints (and exits the program by default) when it sees the locking order being violated.

In addition, if it sees that we are waiting on a lock for a long time (opts.DeadlockTimeout, 30 seconds by default), it reports a potential deadlock, also printing the stacktrace for a goroutine that is currently holding the lock we are desperately trying to grab.


## Sample output
#### Inconsistent lock ordering:
```
POTENTIAL DEADLOCK: Inconsistent locking. saw this ordering in one goroutine:
happened before
inmem.go:623 bttest.(*server).ReadModifyWriteRow { r.mu.Lock() } <<<<<
inmem_test.go:118 bttest.TestConcurrentMutationsReadModifyAndGC.func4 { _, _ = s.ReadModifyWriteRow(ctx, rmw()) }

happened after
inmem.go:629 bttest.(*server).ReadModifyWriteRow { tbl.mu.RLock() } <<<<<
inmem_test.go:118 bttest.TestConcurrentMutationsReadModifyAndGC.func4 { _, _ = s.ReadModifyWriteRow(ctx, rmw()) }

in another goroutine: happened before
inmem.go:799 bttest.(*table).gc { t.mu.RLock() } <<<<<
inmem_test.go:125 bttest.TestConcurrentMutationsReadModifyAndGC.func5 { tbl.gc() }

happend after
inmem.go:814 bttest.(*table).gc { r.mu.Lock() } <<<<<
inmem_test.go:125 bttest.TestConcurrentMutationsReadModifyAndGC.func5 { tbl.gc() }
```

#### Waiting for a lock for a long time:

```
POTENTIAL DEADLOCK:
Previous place where the lock was grabbed
goroutine 240 lock 0xc820160440
inmem.go:799 bttest.(*table).gc { t.mu.RLock() } <<<<<
inmem_test.go:125 bttest.TestConcurrentMutationsReadModifyAndGC.func5 { tbl.gc() }

Have been trying to lock it again for more than 40ms
goroutine 68 lock 0xc820160440
inmem.go:785 bttest.(*table).mutableRow { t.mu.Lock() } <<<<<
inmem.go:428 bttest.(*server).MutateRow { r := tbl.mutableRow(string(req.RowKey)) }
inmem_test.go:111 bttest.TestConcurrentMutationsReadModifyAndGC.func3 { s.MutateRow(ctx, req) }


Here is what goroutine 240 doing now
goroutine 240 [select]:
github.com/sasha-s/go-deadlock.lock(0xc82028ca10, 0x5189e0, 0xc82013a9b0)
        /Users/sasha/go/src/github.com/sasha-s/go-deadlock/deadlock.go:163 +0x1640
github.com/sasha-s/go-deadlock.(*Mutex).Lock(0xc82013a9b0)
        /Users/sasha/go/src/github.com/sasha-s/go-deadlock/deadlock.go:54 +0x86
google.golang.org/cloud/bigtable/bttest.(*table).gc(0xc820160440)
        /Users/sasha/go/src/google.golang.org/cloud/bigtable/bttest/inmem.go:814 +0x28d
google.golang.org/cloud/bigtable/bttest.TestConcurrentMutationsReadModifyAndGC.func5(0xc82015c760, 0xc820160440)      /Users/sasha/go/src/google.golang.org/cloud/bigtable/bttest/inmem_test.go:125 +0x48
created by google.golang.org/cloud/bigtable/bttest.TestConcurrentMutationsReadModifyAndGC
        /Users/sasha/go/src/google.golang.org/cloud/bigtable/bttest/inmem_test.go:126 +0xb6f
```

## Used in
[cockroachdb: Potential deadlock between Gossip.SetStorage and Node.gossipStores](https://github.com/cockroachdb/cockroach/issues/7972)

[bigtable/bttest: A race between GC and row mutations](https://code-review.googlesource.com#/c/5301/)

## Need a mutex that works with net.context?
I have [one](https://github.com/sasha-s/go-csync).

## Grabbing an RLock twice from the same goroutine
This is, surprisingly, not a good idea!

From [RWMutex](https://golang.org/pkg/sync/#RWMutex) docs:

>If a goroutine holds a RWMutex for reading and another goroutine might call Lock, no goroutine should expect to be able to acquire a read lock until the initial read lock is released. In particular, this prohibits recursive read locking. This is to ensure that the lock eventually becomes available; a blocked Lock call excludes new readers from acquiring the lock.


The following code will deadlock &mdash; [run the example on playground](https://play.golang.org/p/AkL-W63nq5f) or [try it online with go-deadlock on wandbox](https://wandbox.org/permlink/JwnL0GMySBju4SII):
```go
package main

import (
	"fmt"
	"sync"
)

func main() {
	var mu sync.RWMutex

	chrlockTwice := make(chan struct{}) // Used to control rlockTwice
	rlockTwice := func() {
		mu.RLock()
		fmt.Println("first Rlock succeeded")
		<-chrlockTwice
		<-chrlockTwice
		fmt.Println("trying to Rlock again")
		mu.RLock()
		fmt.Println("second Rlock succeeded")
		mu.RUnlock()
		mu.RUnlock()
	}

	chLock := make(chan struct{}) // Used to contol lock
	lock := func() {
		<-chLock
		fmt.Println("about to Lock")
		mu.Lock()
		fmt.Println("Lock succeeded")
		mu.Unlock()
		<-chLock
	}

	control := func() {
		chrlockTwice <- struct{}{}
		chLock <- struct{}{}

		close(chrlockTwice)
		close(chLock)
	}

	go control()
	go lock()
	rlockTwice()
}
```
## Build Tags and Compatibility Modes

go-deadlock supports multiple build configurations for different use cases:

* **Normal mode** (default): Full deadlock detection with timer pooling
* **Synctest mode**: Compatible with Go's `testing/synctest` package - use either:
  * `-tags=deadlock_synctest` (recommended for Go 1.25+)
  * `-tags=goexperiment.synctest` (for experimental synctest)
* **Disabled mode** (`-tags=deadlock_disable`): Zero overhead, no detection

**Why synctest mode?** `sync.Mutex` is not durably blocking in synctest bubbles, but channels are. Synctest mode uses channel-based mutexes to ensure proper behavior with `testing/synctest`.

### Quick Examples

```bash
# Normal development/testing
go test ./...

# Testing with synctest (Go 1.25+, recommended)
GODEBUG=asynctimerchan=0 go test -tags=deadlock_synctest ./...

# Testing with experimental synctest
GODEBUG=asynctimerchan=0 go test -tags=goexperiment.synctest ./...

# Production build with zero overhead
go build -tags=deadlock_disable ./...
```

## Configuring go-deadlock

Have a look at [Opts](https://pkg.go.dev/github.com/sasha-s/go-deadlock#pkg-variables).

* `Opts.Disable`: disables deadlock detection altogether (runtime option; see also `deadlock_disable` build tag)
* `Opts.DisableLockOrderDetection`: disables lock order based deadlock detection.
* `Opts.DeadlockTimeout`: blocking on mutex for longer than DeadlockTimeout is considered a deadlock. ignored if negative
* `Opts.OnPotentialDeadlock`: callback for then deadlock is detected
* `Opts.MaxMapSize`: size of happens before // happens after table
* `Opts.PrintAllCurrentGoroutines`:  dump stacktraces of all goroutines when inconsistent locking is detected, verbose
* `Opts.LogBuf`: where to write deadlock info/stacktraces
* `Opts.TimerPool`: controls timer pooling behavior (auto-configured based on build tags)


