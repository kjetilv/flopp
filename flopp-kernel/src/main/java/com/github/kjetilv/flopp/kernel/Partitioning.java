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
    int bufferSize
) {
    public static Partitioning longAligned() {
        return longAligned(0);
    }

    public static Partitioning longAligned(int partitionCount) {
        return new Partitioning(
            partitionCount > 0 ? partitionCount : cpus(),
            LONG_ALIGNMENT,
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
        return new Partitioning(cpus(), 1, bufferSize);
    }

    public Partitioning(int partitionCount, int bufferSize) {
        this(partitionCount, 1, bufferSize);
    }

    public Partitioning {
        Non.negativeOrZero(partitionCount, "partitionCount");
        Non.negativeOrZero(alignment, "alignment");
        Non.negativeOrZero(bufferSize, "bufferSize");
    }

    public List<Partition> of(long total) {
        return partitions(total, partitionCount, alignment);
    }

    public int bufferSizeOr(int defaultSize) {
        return bufferSize > 0 ? bufferSize : defaultSize;
    }

    private static final int DEFAULT_BUFFER = 16 * 1024;

    private static final int LONG_ALIGNMENT = Math.toIntExact(ValueLayout.JAVA_LONG.byteSize());

    private static List<Partition> partitions(long total, long count) {
        return partitions(total, count, 1);
    }

    private static List<Partition> partitions(long total, long count, int alignment) {
        Non.negativeOrZero(count, "count");
        Non.negativeOrZero(total, "total");
        Non.negativeOrZero(alignment, "alignment");
        if (count > total) {
            throw new IllegalStateException(
                STR."Too many partitions for \{total}: \{count} partitions"
            );
        }
        if (total > count) {
            int[] sizes = alignment > 1
                ? partitionSizes(total, count, alignment)
                : partitionSizes(total, count);
            return partitions(count, sizes, alignment);
        }
        return singlePartition(intSized(total), alignment);
    }

    private static int cpus() {
        return Runtime.getRuntime().availableProcessors();
    }

    private static int[] partitionSizes(long total, long count, int alignment) {
        if (total / count < alignment * 2L) {
            throw new IllegalArgumentException(
                STR."Too many partitions for \{total} bytes with alignment \{alignment}: \{count}");
        }
        int overshoot = Math.toIntExact(total % alignment);
        int alignedSlices = Math.toIntExact(total / alignment + 1);
        if (alignedSlices < count) {
            int[] sizes = new int[alignedSlices];
            Arrays.fill(sizes, alignment);
            if (overshoot != 0) {
                sizes[alignedSlices - 1] += overshoot;
            }
            return sizes;
        }
        long totalInFullSlices = total / alignment * alignment;
        int[] sizes = partitionSizes(totalInFullSlices / alignment, count);
        for (int i = 0; i < sizes.length; i++) {
            sizes[i] *= alignment;
        }
        if (overshoot != 0) {
            sizes[Math.toIntExact(count - 1)] += overshoot;
        }
        return sizes;
    }

    private static int[] partitionSizes(long total, long count) {
        int remainders = intSized(total % count);
        int baseCount = intSized(total / count);
        int[] sizes = new int[Math.toIntExact(count)];
        Arrays.fill(sizes, baseCount);
        for (int i = 0; i < remainders; i++) {
            sizes[i] += 1;
        }
        return sizes;
    }

    private static List<Partition> partitions(long count, int[] sizes, int alignment) {
        long offset = 0;
        List<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < sizes.length; i++) {
            partitions.add(
                new Partition(
                    i,
                    Math.toIntExact(count),
                    offset,
                    sizes[i],
                    alignment
                )
            );
            offset += sizes[i];
        }
        return partitions;
    }

    private static List<Partition> singlePartition(int total, int alignment) {
        return List.of(new Partition(0, 1, 0, total, alignment));
    }

    private static int intSized(long count) {
        if (count > MAX_VALUE) {
            throw new IllegalStateException(STR."Expected integer-sized partition: \{count} > \{MAX_VALUE}");
        }
        return Math.toIntExact(count);
    }
}
