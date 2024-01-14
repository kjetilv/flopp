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

public class VectorPartitionSpliterator
    extends Spliterators.AbstractSpliterator<MemorySegments.LineSegment> {

    private final Partition partition;

    private final int partitionLimit;

    private final MemorySegmentSource source;

    private final SurroundConsumer<MemorySegments.LineSegment> lineConsumer;

    private final boolean allocating;

    private final MutableLine segmentLine = new MutableLine();

    private final int partitionNo;

    public VectorPartitionSpliterator(Partition partition, Shape shape, MemorySegmentSource source) {
        this(partition, shape, source, false);
    }

    public VectorPartitionSpliterator(
        Partition partition,
        Shape shape,
        MemorySegmentSource source,
        boolean allocating
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = Objects.requireNonNull(partition, "partition");
        this.partitionLimit = this.partition.count();
        this.partitionNo = partition.partitionNo();
        this.source = Objects.requireNonNull(source, "memorySegmentSources");
        this.allocating = allocating || shape.hasOverhead();

        segmentLine.partitionNo = partition.partitionNo();

        this.lineConsumer = SurroundConsumers.surround(
            this.partition.first() && shape != null && shape.header() > 0 ? shape.header() : 0,
            this.partition.last() && shape != null && shape.footer() > 0 ? shape.footer() : 0
        );
    }

    @Override
    public boolean tryAdvance(Consumer<? super MemorySegments.LineSegment> action) {
        try {
            return process(action);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to process", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{partition}]";
    }

    private boolean process(Consumer<? super MemorySegments.LineSegment> action) {
        MemorySegmentSource.Segment segment = source.get();
        long limit = segment.limit();
        long offset = nextLine(segment, limit);
        long nextLine = offset;
        int skip = 0;

        long linesServed = 0;
        while (true) {
            int shift = 0;
            ByteVector vector;
            try {
                if (offset > limit) {
                    shift = Math.toIntExact(offset - limit);
                }
                vector = vector(segment, Math.min(limit, offset));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            VectorMask<Byte> mask = vector.compare(EQ, '\n');
            if (mask.anyTrue()) {
                int zeroes = Long.numberOfTrailingZeros(mask.toLong() >>> shift);
                int length = skip * mask.length() + zeroes;
                MemorySegments.LineSegment lineSegment = lineSegment(
                    linesServed,
                    segment,
                    nextLine,
                    length
                );
                try {
                    lineConsumer.accept(action, lineSegment);
                } finally {
                    linesServed++;
                }
                offset += zeroes + 1;
                nextLine = offset;
                skip = 0;
                if (exhausted(segment, offset)) {
                    return false;
                }
            } else {
                offset += mask.length();
                skip += 1;
            }
        }
    }

    private long nextLine(MemorySegmentSource.Segment segment, long limit) {
        if (partition.first()) {
            return 0;
        }
        long offset = 0;
        while (true) {
            ByteVector vector = vector(segment, offset);
            VectorMask<Byte> mask = vector.compare(EQ, '\n');
            int leadingZeros = Long.numberOfTrailingZeros(mask.toLong() >>> segment.shift());
            if (leadingZeros == mask.length()) {
                offset += Long.BYTES;
                continue;
            }
            offset += leadingZeros + 1;
            break;
        }
        return offset + segment.shift();
    }

    private ByteVector vector(MemorySegmentSource.Segment segment, long offset) {
        try {
            return ByteVector.fromMemorySegment(
                SPECIES,
                segment.memorySegment(),
                offset,
                NATIVE_ORDER
            );
        } catch (Exception e) {
            throw new IllegalStateException(
                STR."\{this} failed to open vector @ \{offset}: \{segment}", e);
        }
    }

    private boolean exhausted(MemorySegmentSource.Segment segment, long offset) {
        long segmentOffset = offset - segment.shift();
        if (segmentOffset < partitionLimit) {
            return false;
        }
        if (segmentOffset == partitionLimit) {
            return partition.last();
        }
        return true;
    }

    private MemorySegments.LineSegment lineSegment(
        long lineNo,
        MemorySegmentSource.Segment segment,
        long offset,
        int length
    ) {
        if (allocating) {
            return new Line(
                partitionNo,
                lineNo,
                segment.memorySegment(),
                offset,
                length
            );
        }
        segmentLine.memorySegment = segment.memorySegment();
        segmentLine.lineNo = lineNo;
        segmentLine.offset = offset;
        segmentLine.length = length;
        return segmentLine;
    }

    public static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

    private static final VectorSpecies<Byte> SPECIES =
        VectorShape.preferredShape().withLanes(ByteVector.SPECIES_PREFERRED.elementType());

    private record Line(
        int partitionNo,
        long lineNo,
        MemorySegment memorySegment,
        long offset,
        int length
    ) implements MemorySegments.LineSegment {
    }
}
