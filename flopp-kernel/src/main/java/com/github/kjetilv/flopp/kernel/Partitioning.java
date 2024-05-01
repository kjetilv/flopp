package com.github.kjetilv.flopp.kernel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Integer.MAX_VALUE;

public record Partitioning(int count, long tail) {

    public static Partitioning create() {
        return new Partitioning(Runtime.getRuntime().availableProcessors(), 0);
    }

    public static Partitioning create(int count) {
        return create(count, 0);
    }

    public static Partitioning create(int count, long tail) {
        return new Partitioning(
            Non.negativeOrZero(count, "count"),
            Non.negative(tail, "tail")
        );
    }

    public static Partitioning tail(int tail) {
        return create(0, tail);
    }

    public static Partitioning single() {
        return create(1);
    }

    public Partitioning {
        Non.negativeOrZero(count, "partitionCount");
    }

    public Partitioning scaled(double scale) {
        return new Partitioning(
            Math.toIntExact(Math.round(count * scale)),
            tail
        );
    }

    public Partitioning tail(long tail) {
        return new Partitioning(count, tail);
    }

    public List<Partition> of(long total) {
        long reasonablesize = tail + count;
        if (total < reasonablesize) {
            throw new IllegalStateException(this + " requires a length >= " + reasonablesize + ":" + total);
        }
        return partitions(total);
    }

    private List<Partition> partitions(long total) {
        Non.negativeOrZero(total, "total");
        if (count > total) {
            throw new IllegalStateException("Too many partitions for " + total + ": " + count + " partitions");
        }
        if (total > count) {
            long[] sizes = partitionSizes(total);
            return partitions(sizes);
        }
        return singlePartition(total);
    }

    private long[] partitionSizes(long total) {
        if (count == 1) {
            return new long[] {total};
        }
        if (total / count < Partitioning.ALIGNMENT * 2L) {
            throw new IllegalArgumentException(
                "Too many partitions for " + total + " bytes with alignment " + Partitioning.ALIGNMENT + ": " + count);
        }
        if (tail > 0) {
            return alignedSizesWithTail(total);
        }
        return alignedSizes(total);
    }

    private long[] alignedSizesWithTail(long total) {
        long headTotal = total - tail;
        long alignedHeadSlices = headTotal / ALIGNMENT;
        long headOvershoot = headTotal % ALIGNMENT;
        long overshootTail = tail + headOvershoot;
        long[] headSizes = sizeDistribution(alignedHeadSlices);
        long[] sizes = new long[headSizes.length + 1];
        System.arraycopy(headSizes, 0, sizes, 0, headSizes.length);
        sizes[sizes.length - 1] = overshootTail;
        return sizes;
    }

    private long[] alignedSizes(long total) {
        long overshoot = total % ALIGNMENT;
        long alignedSlices = total / ALIGNMENT;
        long[] sizes = sizeDistribution(alignedSlices);
        if (overshoot != 0) {
            sizes[sizes.length - 1] += overshoot;
        }
        return sizes;
    }

    private long[] sizeDistribution(long alignedSlices) {
        long remainders = intSized(alignedSlices % count);
        long baseCount = intSized(alignedSlices / count);
        int sizeCount = Math.toIntExact(count);
        long[] sizes = new long[sizeCount];
        Arrays.fill(sizes, baseCount);
        for (int i = 0; i < remainders; i++) {
            sizes[sizeCount - 1 - i] += 1;
        }
        for (int i = 0; i < sizeCount; i++) {
            sizes[i] *= ALIGNMENT;
        }
        return sizes;
    }

    public static final long ALIGNMENT = 0x08L;

    private static List<Partition> partitions(long[] sizes) {
        long offset = 0;
        List<Partition> partitions = new ArrayList<>(sizes.length);
        for (int i = 0; i < sizes.length; i++) {
            partitions.add(
                new Partition(i, sizes.length, offset, sizes[i])
            );
            offset += sizes[i];
        }
        return partitions;
    }

    private static List<Partition> singlePartition(long total) {
        return List.of(new Partition(0, 1, 0, total));
    }

    private static int intSized(long count) {
        if (count > MAX_VALUE) {
            throw new IllegalStateException("Expected integer-sized partition: " + count + " > " + MAX_VALUE);
        }
        return Math.toIntExact(count);
    }
}
