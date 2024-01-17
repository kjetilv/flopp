package com.github.kjetilv.flopp.kernel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Integer.MAX_VALUE;

public record Partition(int partitionNo, int partitionCount, long offset, long count, int alignment)
    implements Comparable<Partition> {

    public static List<Partition> partitions(long total, long count) {
        return partitions(total, count, 1);
    }

    public static List<Partition> partitions(long total, long  count, int alignment) {
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

    public Partition(int partitionNo, int partitionCount, long offset, long count) {
        this(partitionNo, partitionCount, offset, count, 1);
    }

    public Partition(int partitionNo, int partitionCount, long offset, long count, int alignment) {
        this.partitionNo = Non.negative(partitionNo, "partitionNo");
        this.partitionCount = Non.negativeOrZero(partitionCount, "partitionCount");
        this.offset = Non.negative(offset, "offset");
        this.count = Non.negative(count, "count");
        if (partitionNo >= partitionCount) {
            throw new IllegalStateException(
                STR."partitionNo >= partitionCount: \{partitionNo} >= \{partitionCount}"
            );
        }
        this.alignment = Non.negativeOrZero(alignment, "alignment");
    }

    @Override
    public int compareTo(Partition o) {
        return Integer.compare(partitionNo, o.partitionNo);
    }

    public Partition at(long offset, int count) {
        return new Partition(partitionNo, partitionCount, offset, count);
    }

    public boolean first() {
        return partitionNo == 0;
    }

    public boolean hasData() {
        return count > 0;
    }

    public boolean isAligned(long alignment) {
        return !last() && count % alignment == 0;
    }

    public boolean last() {
        return partitionNo == partitionCount - 1;
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{partitionNo + 1}/\{partitionCount}@\{offset}+\{count}]";
    }

    private static int[] partitionSizes(long total, long count, int alignment) {
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
