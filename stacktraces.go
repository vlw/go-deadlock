package deadlock

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
)

const stackBufSize = 50

var stackBufPool = sync.Pool{
	New: func() any {
		return new([stackBufSize]uintptr)
	},
}

var deadlockPkgPrefix = func() string {
	name := runtime.FuncForPC(reflect.ValueOf(callers).Pointer()).Name()
	return strings.TrimSuffix(name, ".callers")
}()

func isInternalLockFrame(fn string) bool {
	if !strings.HasPrefix(fn, deadlockPkgPrefix+".") {
		return false
	}
	suffix := fn[len(deadlockPkgPrefix)+1:]
	switch suffix {
	case "callers", "captureCallers", "lock", "trylock",
		"(*Mutex).Lock", "(*Mutex).TryLock",
		"(*RWMutex).Lock", "(*RWMutex).TryLock",
		"(*RWMutex).RLock", "(*RWMutex).TryRLock":
		return true
	}
	return false
}

// callers returns a stack trace backed by a pooled buffer. The caller must
// eventually return buf via releaseStackBuf — typically through the
// postLock/postUnlock path which stores it in stackGID.buf.
func callers(skip int) ([]uintptr, *[stackBufSize]uintptr) {
	_ = skip
	buf := stackBufPool.Get().(*[stackBufSize]uintptr)
	n := fpCallers(buf[:])
	return buf[:n], buf
}

// releaseStackBuf returns a pooled stack buffer obtained from callers(). Safe to
// call with nil (e.g. when the buffer was already handed off via stackGID.buf).
func releaseStackBuf(buf *[stackBufSize]uintptr) {
	if buf != nil {
		stackBufPool.Put(buf)
	}
}

// copyStack creates an independent copy of a stack trace. Required when storing
// stacks in long-lived structures (e.g. l.order) because the originals are backed
// by pooled buffers that will be recycled in postUnlock.
func copyStack(s []uintptr) []uintptr {
	c := make([]uintptr, len(s))
	copy(c, s)
	return c
}

func printStack(w io.Writer, stack []uintptr) {
	if len(stack) == 0 {
		fmt.Fprintln(w)
		return
	}
	home := os.Getenv("HOME")
	usr, err := user.Current()
	if err == nil {
		home = usr.HomeDir
	}
	cwd, _ := os.Getwd()

	frames := runtime.CallersFrames(stack)
	printed := 0
	for {
		frame, more := frames.Next()
		if frame.Function == "" && frame.File == "" {
			if !more {
				break
			}
			continue
		}
		if printed == 0 && isInternalLockFrame(frame.Function) {
			if !more {
				break
			}
			continue
		}
		name := frame.Function
		pkg := ""
		if pos := strings.LastIndex(name, "/"); pos >= 0 {
			name = name[pos+1:]
		}
		if pos := strings.Index(name, "."); pos >= 0 {
			pkg = name[:pos]
			name = name[pos+1:]
		}
		if (pkg == "runtime" && name == "goexit") || (pkg == "testing" && name == "tRunner") {
			fmt.Fprintln(w)
			return
		}
		tail := ""
		if printed == 0 {
			tail = " <<<<<" // Make the line performing a lock prominent.
		}
		// Shorten the file name.
		clean := frame.File
		if cwd != "" {
			cl, err := filepath.Rel(cwd, frame.File)
			if err == nil {
				clean = cl
			}
		}
		if home != "" {
			s2 := strings.Replace(frame.File, home, "~", 1)
			if len(clean) > len(s2) {
				clean = s2
			}
		}
		fmt.Fprintf(w, "%s:%d %s.%s %s%s\n", clean, frame.Line, pkg, name, code(frame.File, frame.Line), tail)
		printed++
		if !more {
			break
		}
	}
	fmt.Fprintln(w)
}

var fileSources struct {
	sync.Mutex
	lines map[string][][]byte
}

// Reads source file lines from disk if not cached already.
func getSourceLines(file string) [][]byte {
	fileSources.Lock()
	defer fileSources.Unlock()
	if fileSources.lines == nil {
		fileSources.lines = map[string][][]byte{}
	}
	if lines, ok := fileSources.lines[file]; ok {
		return lines
	}
	text, _ := os.ReadFile(file)
	fileSources.lines[file] = bytes.Split(text, []byte{'\n'})
	return fileSources.lines[file]
}

func code(file string, line int) string {
	lines := getSourceLines(file)
	line -= 1
	if line >= len(lines) || line < 0 {
		return "???"
	}
	return "{ " + string(bytes.TrimSpace(lines[line])) + " }"
}

// Stacktraces for all goroutines.
func stacks() []byte {
	buf := make([]byte, 1024*16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}
