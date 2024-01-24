package com.github.kjetilv.flopp.kernel.bits;

final class Bits {

    static long mask(long l) {
        long masked = l ^ 0x0A0A0A0A0A0A0A0AL;
        long underflown = masked - 0x0101010101010101L;
        long clearedHighBits = underflown & ~masked;
        return clearedHighBits & 0x8080808080808080L;
    }

    static int trailingBytes(long mask) {
        return Long.numberOfTrailingZeros(mask) / 8 + 1;
    }

    private Bits() {
    }
}
