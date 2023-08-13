package com.github.kjetilv.lopp;

import com.github.kjetilv.lopp.utils.Non;

import java.util.ArrayList;
import java.util.List;

public record Partition(int partitionNo, int partitionCount, long offset, long count)
    implements Comparable<Partition> {

    public Partition(
        int partitionNo,
        int partitionCount,
        long offset,
        long count
    ) {
        this.partitionNo = Non.negative(partitionNo, "partitionNo");
        this.partitionCount = Non.negativeOrZero(partitionCount, "partitionCount");
        this.offset = Non.negative(offset, "offset");
        this.count = Non.negative(count, "count");
    }

    public static List<Partition> partitions(long total, int partitionCount) {
        return partitions(total, null, partitionCount);
    }

    public static List<Partition> partitions(
        long total,
        FileShape fileShape,
        int partitionCount
    ) {
        return createPartitions(
            Non.negativeOrZero(total, "total"),
            fileShape,
            Non.negativeOrZero(partitionCount, "partitionCount")
        );
    }

    private static List<Partition> createPartitions(
        long total,
        FileShape fileShape,
        int partitionCount
    ) {
        long retrievableLineCount = retrievableLineCount(total, fileShape, partitionCount);
        if (retrievableLineCount <= partitionCount) {
            return List.of(new Partition(0, 1, 0, retrievableLineCount));
        }
        int remainders = Math.toIntExact(retrievableLineCount % partitionCount);
        long baseCount = retrievableLineCount / partitionCount;
        long[] sizes = new long[partitionCount];
        for (int i = 0; i < remainders; i++) {
            sizes[i] = baseCount + 1;
        }
        for (int i = remainders; i < partitionCount; i++) {
            sizes[i] = baseCount;
        }
        long offset = 0;
        List<Partition> partitions = new ArrayList<>();
        int header = fileShape == null ? 0 : fileShape.decor().header();
        for (int i = 0; i < sizes.length; i++) {
            partitions.add(
                new Partition(i, partitionCount, offset + header, sizes[i])
            );
            offset += sizes[i];
        }
        return partitions;
    }

    private static long retrievableLineCount(
        long total,
        FileShape fileShape,
        int partitionCount
    ) {
        int decorSize = fileShape == null ? 0 : fileShape.decor().size();
        long retrievableLineCount = total - decorSize;
        if (retrievableLineCount <= 0) {
            throw new IllegalStateException(
                "No lines to retrieve, total = " + total + ", header/footer = " + decorSize);
        }
        if (partitionCount > retrievableLineCount) {
            throw new IllegalStateException(
                "Too many partitions for " + retrievableLineCount + " lines: " + partitionCount + " partitions");
        }
        return retrievableLineCount;
    }

    @Override
    public int compareTo(Partition o) {
        return Integer.compare(partitionNo, o.partitionNo);
    }

    public PartitionBytes at(long offset, long count) {
        return new PartitionBytes(offset, count, this);
    }

    public boolean first() {
        return partitionNo == 0;
    }

    public boolean empty() {
        return count == 0;
    }

    public boolean last() {
        return partitionNo == partitionCount - 1;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[#" + partitionNo +
            "{" + (partitionNo + 1) + "/" + partitionCount + "}@" + offset + "+" + count + "]";
    }
}
