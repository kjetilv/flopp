package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.util.Non;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.github.kjetilv.flopp.kernel.segments.MemorySegments.ALIGNMENT;
import static com.github.kjetilv.flopp.kernel.segments.MemorySegments.ALIGNMENT_POW;
import static java.lang.Integer.MAX_VALUE;

public record Partitioning(int count, long tail, TailShards fragmentation) {

    public static Partitioning create() {
        return new Partitioning(Runtime.getRuntime().availableProcessors(), 0, null);
    }

    public static Partitioning create(int count) {
        return create(count, 0);
    }

    public static Partitioning create(int count, long tail) {
        return new Partitioning(
            Non.negativeOrZero(count, "count"),
            Non.negative(tail, "tail"),
            null
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
            tail,
            null
        );
    }

    public Partitioning tail(long tail) {
        return new Partitioning(count, tail, fragmentation);
    }

    public Partitioning fragment(
        int shardCount,
        double tailPerc,
        double partitionMaxPerc,
        double partitionMinPerc
    ) {
        return fragment(new TailShards(
            shardCount,
            tailPerc,
            partitionMaxPerc,
            partitionMinPerc
        ));
    }

    public Partitions of(long total) {
        checkSize(total);
        if (fragmentation == null) {
            List<Partition> partitions = total > count
                ? partitions(partitionSizes(count, total, tail))
                : singlePartition(total);
            return new Partitions(total, partitions, tail);
        }
        if (total > count) {
            Partitions fragmentedPartitions = fragmentation.create(total, count, tail);
            long mainTotal = total - fragmentedPartitions.total();

            long[] sizes = partitionSizes(count, mainTotal, tail);
            List<Partition> partitions = partitions(sizes);

            Partitions mainPart = new Partitions(mainTotal, partitions, tail);
            return mainPart.insertAtEnd(fragmentedPartitions);
        }
        List<Partition> partitions = singlePartition(total);
        return new Partitions(total, partitions, tail);
    }

    public Partitioning fragment(TailShards tailShards) {
        return new Partitioning(count, tail, tailShards);
    }

    private void checkSize(long total) {
        Non.negativeOrZero(total, "total");
        if (count > 1) {
            long reasonableSize = tail + count;
            if (total < reasonableSize) {
                throw new IllegalStateException(
                    this + " requires a length >= " + reasonableSize + ", total size is " + total);
            }
            if (count > total) {
                throw new IllegalStateException(
                    "Too many partitions for " + total + ": " + count + " partitions");
            }
        }
    }

    private static long[] partitionSizes(int count, long total, long tail) {
        if (count == 1) {
            return new long[] {total};
        }
        if (total / count < ALIGNMENT * 2L) {
            throw new IllegalArgumentException(
                "Too many partitions for " + total + " bytes with alignment " + ALIGNMENT + ": " + count
            );
        }
        return tail > 0
            ? alignedSizesWithTail(count, total, tail)
            : alignedSizes(count, total);
    }

    private static long[] alignedSizesWithTail(int count, long total, long tail) {
        long headTotal = total - tail;
        long alignedHeadSlices = headTotal >> ALIGNMENT_POW;
        long headOvershoot = headTotal % ALIGNMENT;
        long overshootTail = tail + headOvershoot;
        long[] headSizes = sizeDistribution(alignedHeadSlices, count);
        long[] sizes = new long[headSizes.length + 1];
        System.arraycopy(headSizes, 0, sizes, 0, headSizes.length);
        sizes[sizes.length - 1] = overshootTail;
        return sizes;
    }

    private static long[] alignedSizes(int count, long total) {
        long overshoot = total % ALIGNMENT;
        long alignedSlices = total >> ALIGNMENT_POW;
        long[] sizes = sizeDistribution(alignedSlices, count);
        if (overshoot != 0) {
            sizes[sizes.length - 1] += overshoot;
        }
        return sizes;
    }

    private static long[] sizeDistribution(long alignedSlices, int count) {
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
