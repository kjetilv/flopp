package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.ValueLayout;

final class Bits {

    private Bits() {

    }

    static final int ALIGNMENT_INT = Math.toIntExact(ValueLayout.JAVA_LONG.byteSize());

    static final long ALIGNMENT = ALIGNMENT_INT;
}
