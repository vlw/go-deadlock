//go:build (amd64 || arm64) && !deadlock_no_fpunwind

package deadlock

// fpCallers walks the goroutine frame-pointer chain and stores physical return
// PCs into pc, returning the number of frames captured.
//
//go:noescape
func fpCallers(pc []uintptr) int
