package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Integer.MAX_VALUE;

@SuppressWarnings("unused")
public record Partitioning(
    int partitionCount,
    int alignment,
    long shortTail,
    int bufferSize
) {
    public static Partitioning longAligned() {
        return longAligned(0);
    }

    public static Partitioning longAligned(int partitionCount) {
        return longAligned(partitionCount, 0);
    }

    public static Partitioning longAligned(int partitionCount, long shortTail) {
        return new Partitioning(
            Non.negative(partitionCount, "partitionCount") > 0
                ? partitionCount
                : cpus(),
            LONG_ALIGNMENT,
            Non.negative(shortTail, "shortTail"),
            DEFAULT_BUFFER
        );
    }

    public static Partitioning count(int partitionCount) {
        return new Partitioning(partitionCount, DEFAULT_BUFFER);
    }

    public static Partitioning defaults() {
        return defaults(DEFAULT_BUFFER);
    }

    public static Partitioning defaults(int bufferSize) {
        return new Partitioning(cpus(), 1, 0, bufferSize);
    }

    public Partitioning(int partitionCount, int bufferSize) {
        this(partitionCount, 1, 0, bufferSize);
    }

    public Partitioning {
        Non.negativeOrZero(partitionCount, "partitionCount");
        Non.negativeOrZero(alignment, "alignment");
        Non.negativeOrZero(bufferSize, "bufferSize");
    }

    public int partitionCount(boolean tailed) {
        return partitionCount + (tailed && shortTail > 0 ? 1 : 0);
    }

    public List<Partition> of(long total) {
        return partitions(total);
    }

    public int bufferSizeOr(int defaultSize) {
        return bufferSize > 0 ? bufferSize : defaultSize;
    }

    private List<Partition> partitions(long total) {
        Non.negativeOrZero(total, "total");
        if (partitionCount > total) {
            throw new IllegalStateException(
                STR."Too many partitions for \{total}: \{partitionCount} partitions"
            );
        }
        if (total > partitionCount) {
            return partitions(partitionSizes(total));
        }
        return singlePartition(total);
    }

    private long[] partitionSizes(long total) {
        if (alignment > 1 && total / partitionCount < alignment * 2L) {
            throw new IllegalArgumentException(
                STR."Too many partitions for \{total} bytes with alignment \{alignment}: \{partitionCount}");
        }
        if (alignment > 1 && shortTail > 0) {
            return alignedSizesWithShortTail(total);
        }
        if (alignment > 1) {
            return alignedSizes(total);
        }
        return defaultDistributed(total);
    }

    private long[] alignedSizes(long total) {
        long overshoot = total % alignment;
        long alignedSlices = total / alignment;
        long[] sizes = defaultDistributedAlignmentScaled(alignedSlices);
        if (overshoot != 0) {
            sizes[sizes.length - 1] += overshoot;
        }
        return sizes;
    }

    private long[] alignedSizesWithShortTail(long total) {
        long overshoot = total % alignment;
        long alignedSlices = total / alignment;
        if (overshoot == 0) {
            return defaultDistributedAlignmentScaled(alignedSlices);
        }
        long headTotal = total - shortTail;
        long alignedHeadSlices = headTotal / alignment;
        long headOvershoot = headTotal % alignment;
        long overshootTail = shortTail + headOvershoot;
        long[] headSizes = defaultDistributedAlignmentScaled(alignedHeadSlices);
        long[] sizes = new long[headSizes.length + 1];
        System.arraycopy(headSizes, 0, sizes, 0, headSizes.length);
        sizes[sizes.length - 1] = overshootTail;
        return sizes;
    }

    private long[] defaultDistributedAlignmentScaled(long alignedSlices) {
        long[] sizes = defaultDistributed(alignedSlices);
        for (int i = 0; i < sizes.length; i++) {
            sizes[i] *= alignment;
        }
        return sizes;
    }

    private long[] defaultDistributed(long total) {
        long remainders = intSized(total % partitionCount);
        long baseCount = intSized(total / partitionCount);
        long[] sizes = new long[Math.toIntExact(partitionCount)];
        Arrays.fill(sizes, baseCount);
        for (int i = 0; i < remainders; i++) {
            sizes[i] += 1;
        }
        return sizes;
    }

    private List<Partition> partitions(long[] sizes) {
        long offset = 0;
        int additional = shortTail > 0 ? 1 : 0;
        List<Partition> partitions = new ArrayList<>(sizes.length + additional);
        int count = partitionCount + additional;
        for (int i = 0; i < sizes.length; i++) {
            partitions.add(
                new Partition(i, count, offset, sizes[i], alignment)
            );
            offset += sizes[i];
        }
        return partitions;
    }

    private List<Partition> singlePartition(long total) {
        return List.of(new Partition(0, 1, 0, total, alignment));
    }

    private static final int DEFAULT_BUFFER = 16 * 1024;

    private static final int LONG_ALIGNMENT = Math.toIntExact(ValueLayout.JAVA_LONG.byteSize());

    private static int cpus() {
        return Runtime.getRuntime().availableProcessors();
    }

    private static int intSized(long count) {
        if (count > MAX_VALUE) {
            throw new IllegalStateException(STR."Expected integer-sized partition: \{count} > \{MAX_VALUE}");
        }
        return Math.toIntExact(count);
    }
}
