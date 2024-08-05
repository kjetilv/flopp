package com.github.kjetilv.flopp.kernel;

import java.util.ArrayList;
import java.util.List;

record TailShards(int shardCount, double tailPerc, double partitionMaxPerc, double partitionMinPerc) {

    TailShards(
        int shardCount,
        double tailPerc,
        double partitionMaxPerc,
        double partitionMinPerc
    ) {
        if (tailPerc > 10) {
            throw new IllegalStateException("Tail too big: %" + tailPerc);
        }
        this.shardCount = Non.negativeOrZero(shardCount, "shardCount");
        this.tailPerc = Non.negativeOrZero(tailPerc, "tailPercentage");
        this.partitionMinPerc = Non.negativeOrZero(partitionMinPerc, "partitionMinPercentage");
        this.partitionMaxPerc = Non.negativeOrZero(partitionMaxPerc, "partitionMaxPercentage");
    }

    Partitions create(long total, int count, long tail) {
        long approxTailSize = Math.round(tailPerc / 100 * total);
        long approxMainSize = (total - approxTailSize) / count;

        long percentageMinSize = Math.round(partitionMinPerc / 100 * approxMainSize);
        long tailAdjustedSize = Math.min(tail * 5, approxMainSize / 10);
        long minSize = tail > 0 ? tailAdjustedSize : percentageMinSize;

        List<Partition> list = partitions(approxTailSize, minSize);
        long sum = list.stream().mapToLong(Partition::length).sum();
        return new Partitions(sum, list, 0L);
    }

    private List<Partition> partitions(long tailSize, long minSize) {
        int blocksCount = (shardCount + 1) * shardCount;
        long blockTotal = tailSize - shardCount * minSize;
        long blockSize = blockTotal / blocksCount;

        List<Partition> list = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            long blocks = (shardCount - i) * blockSize;
            long offset = i == 0 ? 0L : list.getLast().endIndex();
            long alignedSize = alignedSize(minSize + blocks);
            list.add(new Partition(i, shardCount, offset, alignedSize));
        }
        return list;
    }

    private static final int ALIGNED = 16;

    private static long alignedSize(long availableRaw) {
        return availableRaw - availableRaw % ALIGNED;
    }
}
