//go:build amd64 && !deadlock_no_fpunwind

#include "textflag.h"

// func fpCallers(pc []uintptr) int
//
// Walks the goroutine frame-pointer chain and writes physical return PCs into
// pc, returning the number of entries written. Logical inlined frames are
// expanded lazily by runtime.CallersFrames when printing a deadlock report.
TEXT ·fpCallers(SB), NOSPLIT|NOFRAME, $0-32
	MOVQ	pc_base+0(FP), DI	// dst = &pc[0]
	MOVQ	pc_len+8(FP), CX	// limit = len(pc)
	XORQ	AX, AX			// n = 0
	TESTQ	CX, CX
	JLE	done

	// Record the immediate return PC pushed by our caller's CALL instruction.
	MOVQ	0(SP), DX
	TESTQ	DX, DX
	JEQ	done
	MOVQ	DX, (DI)
	INCQ	AX
	CMPQ	AX, CX
	JGE	done

	// Walk caller frame pointers starting from the caller's BP.
	// On amd64, [BP+0] holds the previous BP and [BP+8] holds the return PC.
	MOVQ	BP, BX
	TESTQ	BX, BX
	JEQ	done
	TESTQ	$7, BX
	JNE	done

loop:
	MOVQ	8(BX), DX		// retPC = *(BX + 8)
	TESTQ	DX, DX
	JEQ	done
	MOVQ	DX, (DI)(AX*8)		// pc[n] = retPC
	INCQ	AX
	CMPQ	AX, CX
	JGE	done

	MOVQ	0(BX), SI		// nextFP = *BX
	TESTQ	SI, SI
	JEQ	done
	CMPQ	SI, BX			// stack grows down: nextFP must be > fp
	JBE	done
	TESTQ	$7, SI			// must remain 8-byte aligned
	JNE	done
	MOVQ	SI, DX
	SUBQ	BX, DX
	CMPQ	DX, $16777216		// sanity cap: single frame <= 16 MiB
	JA	done

	MOVQ	SI, BX
	JMP	loop

done:
	MOVQ	AX, ret+24(FP)
	RET
