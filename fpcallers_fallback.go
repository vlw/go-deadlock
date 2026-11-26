//go:build (!amd64 && !arm64) || deadlock_no_fpunwind

package deadlock

import "runtime"

func fpCallers(pc []uintptr) int {
	return runtime.Callers(2, pc)
}
