package com.github.kjetilv.flopp.kernel.util;

import com.github.kjetilv.flopp.kernel.Vectors;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;

import static java.nio.ByteOrder.nativeOrder;

public final class MemorySegmentByteFinder implements Vectors.ByteFinder {

    private final MemorySegment segment;

    private final byte character;

    private final long lastBound;

    private final long bound;

    private long nextOffset;

    private VectorMask<Byte> mask;

    public MemorySegmentByteFinder(MemorySegment segment, byte character) {
        this(segment, 0L, character);
    }

    public MemorySegmentByteFinder(MemorySegment segment, long initialOffset, char character) {
        this(segment, initialOffset, (byte) character);
    }

    public MemorySegmentByteFinder(MemorySegment segment, long initialOffset, byte character) {
        this.segment = Objects.requireNonNull(segment, "memorySegment");
        this.nextOffset = Non.negative(initialOffset, "offset");
        this.bound = this.segment.byteSize() - VLEN;
        this.lastBound = bound + VLEN;
        this.character = Non.negative(character, "byte");
        this.mask = this.cycle();
    }

    @Override
    public long next() {
        int next;
        while ((next = mask.firstTrue()) == VLEN) {
            if (this.nextOffset < this.lastBound) {
                this.mask = this.cycle();
            } else {
                return -1L;
            }
        }
        this.mask = this.mask.and(ZEROES[next]);
        return this.nextOffset - VLEN + next;
    }

    private VectorMask<Byte> cycle() {
        try {
            return this.nextOffset > this.bound
                ? this.tailMask()
                : this.maskAt(nextOffset);
        } finally {
            nextOffset += VLEN;
        }
    }

    private VectorMask<Byte> tailMask() {
        boolean[] tail = new boolean[VLEN];
        Arrays.fill(tail, false);
        VectorMask<Byte> tailVector = maskAt(bound);
        int consumed = (int) (this.nextOffset - this.bound);
        int vectorLength = VLEN - consumed;
        for (int i = 0; i < vectorLength; i++) {
            tail[i] = tailVector.laneIsSet(i + consumed);
        }
        return VectorMask.fromValues(SPECIES, tail);
    }

    private VectorMask<Byte> maskAt(long offset) {
        return ByteVector.fromMemorySegment(SPECIES, segment, offset, BO).eq(character);
    }

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_PREFERRED;

    private static final int VLEN = SPECIES.length();

    private static final VectorMask<Byte>[] ZEROES;

    private static final ByteOrder BO = nativeOrder();

    static {
        boolean[] b = new boolean[SPECIES.length()];
        Arrays.fill(b, true);
        ZEROES = IntStream.range(0, SPECIES.length())
            .mapToObj(i -> {
                b[i] = false;
                return VectorMask.fromValues(SPECIES, b);
            })
            .<VectorMask<Byte>>toArray(VectorMask[]::new);
    }
}
