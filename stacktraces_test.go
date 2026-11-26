package deadlock

import (
	"bytes"
	"runtime"
	"strings"
	"testing"
)

func capturePair() (fpOut, rtOut string) {
	var fpBuf, rtBuf [stackBufSize]uintptr
	nFP := fpCallers(fpBuf[:])
	nRT := runtime.Callers(1, rtBuf[:])
	var bFP, bRT bytes.Buffer
	printStack(&bFP, fpBuf[:nFP])
	printStack(&bRT, rtBuf[:nRT])
	return bFP.String(), bRT.String()
}

// inlineHelper is intentionally small so the compiler inlines it into its caller.
func inlineHelper(capture func() (string, string)) (string, string) {
	return capture()
}

//go:noinline
func noinlineHelper(capture func() (string, string)) (string, string) {
	return capture()
}

func genericHelper[T any](_ T, capture func() (string, string)) (string, string) {
	return capture()
}

// stripFirstLine removes the capturePair line itself, which differs by 1 line
// because fpCallers and runtime.Callers are invoked on consecutive lines.
func stripFirstLine(s string) string {
	idx := strings.IndexByte(s, '\n')
	if idx < 0 {
		return s
	}
	return s[idx+1:]
}

func TestFPCallersMatchesRuntimeCallers(t *testing.T) {
	cases := []struct {
		name string
		run  func() (string, string)
	}{
		{
			name: "direct",
			run:  func() (string, string) { return capturePair() },
		},
		{
			name: "inlined",
			run:  func() (string, string) { return inlineHelper(capturePair) },
		},
		{
			name: "noinline",
			run:  func() (string, string) { return noinlineHelper(capturePair) },
		},
		{
			name: "closure",
			run: func() (string, string) {
				fn := func() (string, string) { return capturePair() }
				return fn()
			},
		},
		{
			name: "generic",
			run:  func() (string, string) { return genericHelper(42, capturePair) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fpOut, rtOut := tc.run()
			if fpOut == "" {
				t.Fatal("fpCallers produced empty stack output")
			}
			got := stripFirstLine(fpOut)
			want := stripFirstLine(rtOut)
			if got != want {
				t.Fatalf("stack mismatch:\n--- fpCallers ---\n%s--- runtime.Callers ---\n%s", got, want)
			}
		})
	}
}

func TestPrintStackPointsToUserLockLine(t *testing.T) {
	defer restore()()
	Opts.DeadlockTimeout = 0
	var logBuf bytes.Buffer
	Opts.LogBuf = &logBuf
	Opts.OnPotentialDeadlock = func() {}

	var a, b Mutex
	a.Lock()
	b.Lock()
	b.Unlock()
	a.Unlock()

	b.Lock()
	a.Lock()
	a.Unlock()
	b.Unlock()

	out := logBuf.String()
	if !strings.Contains(out, "{ a.Lock() } <<<<<") {
		t.Fatalf("expected stack trace to highlight { a.Lock() } <<<<<, got:\n%s", out)
	}
	if !strings.Contains(out, "{ b.Lock() } <<<<<") {
		t.Fatalf("expected stack trace to highlight { b.Lock() } <<<<<, got:\n%s", out)
	}
	if strings.Contains(out, "deadlock.(*Mutex).Lock") {
		t.Fatalf("expected internal deadlock frames to be stripped, got:\n%s", out)
	}
}
