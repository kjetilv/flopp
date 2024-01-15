package com.github.kjetilv.flopp.kernel;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.MAX_VALUE;

public record Partition(int partitionNo, int partitionCount, long offset, int count)
    implements Comparable<Partition> {

    public static List<Partition> partitions(long total, int count) {
        if (Non.negativeOrZero(count, "count") > Non.negativeOrZero(total, "total")) {
            throw new IllegalStateException(
                "Too many partitions for " + total + ": " + count + " partitions");
        }
        if (total > count) {
            int[] sizes = partitionSizes(total, count);
            return partitions(count, sizes);
        }
        return singlePartition(intSized(total));
    }

    public Partition(
        int partitionNo,
        int partitionCount,
        long offset,
        long count
    ) {
        this(partitionNo, partitionCount, offset, intSized(count));
    }

    public Partition(
        int partitionNo,
        int partitionCount,
        long offset,
        int count
    ) {
        this.partitionNo = Non.negative(partitionNo, "partitionNo");
        this.partitionCount = Non.negativeOrZero(partitionCount, "partitionCount");
        this.offset = Non.negative(offset, "offset");
        this.count = Non.negative(count, "count");
        if (partitionNo >= partitionCount) {
            throw new IllegalStateException("partitionNo >= partitionCount: " + partitionNo + " >= " + partitionCount);
        }
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

    public boolean last() {
        return partitionNo == partitionCount - 1;
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{partitionNo + 1}/\{partitionCount}@\{offset}+\{count}]";
    }

    private static int[] partitionSizes(long total, int count) {
        int remainders = intSized(total % count);
        int baseCount = intSized(total / count);
        int[] sizes = new int[count];
        for (int i = 0; i < remainders; i++) {
            sizes[i] = baseCount + 1;
        }
        for (int i = remainders; i < count; i++) {
            sizes[i] = baseCount;
        }
        return sizes;
    }

    private static List<Partition> partitions(int count, int[] sizes) {
        long offset = 0;
        List<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < sizes.length; i++) {
            partitions.add(
                new Partition(i, count, offset, sizes[i])
            );
            offset += sizes[i];
        }
        return partitions;
    }

    private static List<Partition> singlePartition(int total) {
        return List.of(new Partition(0, 1, 0, total));
    }

    private static int intSized(long count) {
        if (count > MAX_VALUE) {
            throw new IllegalStateException("Expected integer-sized partition: " + count + " > " + MAX_VALUE);
        }
        return Math.toIntExact(count);
    }
}
