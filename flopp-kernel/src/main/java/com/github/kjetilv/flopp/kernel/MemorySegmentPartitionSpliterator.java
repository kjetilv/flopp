package com.github.kjetilv.flopp.kernel;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

import static jdk.incubator.vector.VectorOperators.EQ;

public class MemorySegmentPartitionSpliterator
    extends Spliterators.AbstractSpliterator<MemorySegments.Line> {

    private final Partition partition;

    private final long limit;

    private final MemorySegment memorySegment;

    public MemorySegmentPartitionSpliterator(Partition partition, MemorySegmentSources memorySegmentSources) {
        this(partition, memorySegment(partition, memorySegmentSources));
    }

    public MemorySegmentPartitionSpliterator(Partition partition, MemorySegment memorySegment) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = partition;
        this.memorySegment = memorySegment;
        this.limit = memorySegment.byteSize() - SPECIES.length();
    }

    @Override
    public boolean tryAdvance(Consumer<? super MemorySegments.Line> action) {
        long offset = partition.offset();
        while (true) {
            ByteVector vector = ByteVector.fromMemorySegment(
                SPECIES,
                memorySegment,
                offset,
                ByteOrder.nativeOrder()
            );
            VectorMask<Byte> mask = vector.compare(EQ, '\n');
            int leadingZeros = Long.numberOfTrailingZeros(mask.toLong());
            if (leadingZeros == mask.length()) {
                offset += Long.BYTES;
                continue;
            }
            offset += leadingZeros + 1;
            break;
        }
        long lastLine = offset;
        int skip = 0;
        while (true) {
            ByteVector vector;
            long shift = 0;
            try {
                long actualOffset = Math.min(limit, offset);
                if (offset > limit) {
                    shift = offset - limit;
                }
                vector = ByteVector.fromMemorySegment(
                    SPECIES,
                    memorySegment,
                    actualOffset,
                    ByteOrder.nativeOrder()
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            VectorMask<Byte> mask = vector.compare(EQ, '\n');
            int maskLength = mask.length();
            int zeroes = Long.numberOfTrailingZeros(mask.toLong() >>> shift);
            if (zeroes < BITS_IN_LONG) {
                int length = skip * maskLength + zeroes;
                action.accept(new SegmentLine(memorySegment, lastLine, length));
                offset += zeroes + 1;
                lastLine = offset;
                skip = 0;
                if (offset >= partition.count()) {
                    break;
                }
            } else {
                offset += maskLength;
                skip += 1;
            }
        }
        return false;
    }

    private static final int BITS_IN_LONG = Long.BYTES * 8;

    private static final VectorSpecies<Byte> SPECIES =
        VectorShape.preferredShape().withLanes(ByteVector.SPECIES_PREFERRED.elementType());

    private static MemorySegment memorySegment(Partition partition, MemorySegmentSources memorySegmentSources) {
        return Objects.requireNonNull(
            memorySegmentSources,
            "memorySegmentSources"
        ).source(partition).get();
    }

    private record SegmentLine(MemorySegment memorySegment, long offset, int length) implements MemorySegments.Line {
    }
}
