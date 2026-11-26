//go:build arm64 && !deadlock_no_fpunwind

#include "textflag.h"

// func fpCallers(pc []uintptr) int
//
// Walks the goroutine frame-pointer chain and writes physical return PCs into
// pc, returning the number of entries written. Logical inlined frames are
// expanded lazily by runtime.CallersFrames when printing a deadlock report.
TEXT ·fpCallers(SB), NOSPLIT|NOFRAME, $0-32
	MOVD	pc_base+0(FP), R0	// dst = &pc[0]
	MOVD	pc_len+8(FP), R1	// limit = len(pc)
	MOVD	$0, R2			// n = 0
	CMP	$0, R1
	BLE	done

	// Record the immediate return PC in LR (R30).
	CBZ	R30, done
	MOVD	R30, (R0)
	ADD	$1, R2
	CMP	R1, R2
	BGE	done

	// Walk caller frame pointers starting from FP (R29).
	// On arm64, [FP+0] holds the previous FP and [FP+8] holds the saved LR.
	MOVD	R29, R3
	CBZ	R3, done
	TST	$7, R3
	BNE	done

loop:
	MOVD	8(R3), R4		// retPC = *(FP + 8)
	CBZ	R4, done
	MOVD	R4, (R0)(R2<<3)		// pc[n] = retPC
	ADD	$1, R2
	CMP	R1, R2
	BGE	done

	MOVD	0(R3), R5		// nextFP = *FP
	CBZ	R5, done
	CMP	R3, R5			// stack grows down: nextFP must be > fp
	BLS	done
	TST	$7, R5			// must remain 8-byte aligned
	BNE	done
	SUB	R3, R5, R6
	MOVD	$16777216, R7		// sanity cap: single frame <= 16 MiB
	CMP	R7, R6
	BHI	done

	MOVD	R5, R3
	B	loop

done:
	MOVD	R2, ret+24(FP)
	RET
