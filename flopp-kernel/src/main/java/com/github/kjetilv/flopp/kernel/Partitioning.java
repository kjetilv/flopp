package com.github.kjetilv.flopp.kernel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Integer.MAX_VALUE;

@SuppressWarnings("unused")
public record Partitioning(int count, long tail) {

    public static Partitioning create() {
        return create(0);
    }

    public static Partitioning create(int partitionCount) {
        return create(partitionCount, 0);
    }

    public static Partitioning create(int partitionCount, long tail) {
        return new Partitioning(
            Non.negative(partitionCount, "partitionCount") > 0
                ? partitionCount
                : cpus(),
            Non.negative(tail, "tailSize")
        );
    }

    public Partitioning {
        Non.negativeOrZero(count, "partitionCount");
    }

    public Partitioning scaled(int scale) {
        return new Partitioning(
            count * scale,
            tail
        );
    }

    public Partitioning tail(long tail) {
        return new Partitioning(count, tail);
    }

    public List<Partition> of(long total) {
        return checked(partitions(total));
    }

    private List<Partition> partitions(long total) {
        Non.negativeOrZero(total, "total");
        if (count > total) {
            throw new IllegalStateException(
                STR."Too many partitions for \{total}: \{count} partitions"
            );
        }
        if (total > count) {
            long[] sizes = partitionSizes(total);
            return partitions(sizes);
        }
        return singlePartition(total);
    }

    private long[] partitionSizes(long total) {
        int alignment = 8;
        if (total / count < alignment * 2L) {
            throw new IllegalArgumentException(
                STR."Too many partitions for \{total} bytes with alignment \{alignment}: \{count}");
        }
        if (tail > 0) {
            return alignedSizesWithTail(total);
        }
        return alignedSizes(total);
    }

    private long[] alignedSizes(long total) {
        int alignment = 8;
        long overshoot = total % alignment;
        long alignedSlices = total / alignment;
        long[] sizes = defaultDistributedAlignmentScaled(alignedSlices);
        if (overshoot != 0) {
            sizes[sizes.length - 1] += overshoot;
        }
        return sizes;
    }

    private long[] alignedSizesWithTail(long total) {
        long overshoot = total % ALIGNMENT;
        long alignedSlices = total / ALIGNMENT;
        if (overshoot == 0 && tail == 0) {
            return defaultDistributedAlignmentScaled(alignedSlices);
        }
        long headTotal = total - tail;
        long alignedHeadSlices = headTotal / ALIGNMENT;
        long headOvershoot = headTotal % ALIGNMENT;
        long overshootTail = tail + headOvershoot;
        long[] headSizes = defaultDistributedAlignmentScaled(alignedHeadSlices);
        long[] sizes = new long[headSizes.length + 1];
        System.arraycopy(headSizes, 0, sizes, 0, headSizes.length);
        sizes[sizes.length - 1] = overshootTail;
        return sizes;
    }

    private long[] defaultDistributedAlignmentScaled(long alignedSlices) {
        long[] sizes = defaultDistributed(alignedSlices);
        for (int i = 0; i < sizes.length; i++) {
            sizes[i] *= ALIGNMENT;
        }
        return sizes;
    }

    private long[] defaultDistributed(long total) {
        long remainders = intSized(total % count);
        long baseCount = intSized(total / count);
        long[] sizes = new long[Math.toIntExact(count)];
        Arrays.fill(sizes, baseCount);
        for (int i = 0; i < remainders; i++) {
            sizes[i] += 1;
        }
        return sizes;
    }

    public static final int ALIGNMENT = 8;

    private static final int DEFAULT_BUFFER = 16 * 1024;

    private static List<Partition> checked(List<Partition> partitions) {
        if (!partitions.getFirst().first()) {
            throw new IllegalStateException(STR."First not first: \{partitions}");
        }
        if (!partitions.getLast().last()) {
            throw new IllegalStateException(STR."First not first: \{partitions}");
        }
        partitions.stream().skip(1)
            .forEach(partition -> {
                if (partition.first()) {
                    throw new IllegalStateException(STR."\{partition} is first");
                }
            });
        partitions.stream().limit(partitions.size() - 1)
            .forEach(partition -> {
                if (partition.last()) {
                    throw new IllegalStateException(STR."\{partition} is last");
                }
            });
        return partitions;
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
