package com.github.kjetilv.flopp.kernel;

import java.util.ArrayList;
import java.util.List;

public record TrailFragmentation(
    int trailCount,
    double trailPercentage,
    double partitionMinPercentage,
    double partitionMaxPercentage
) {

    public TrailFragmentation {
        if (trailPercentage > 99) {
            throw new IllegalStateException("Too high trail %: " + trailPercentage);
        }
        Non.negativeOrZero(trailCount, "trailCount");
        Non.negativeOrZero(trailPercentage, "trailPercentage");
        Non.negativeOrZero(partitionMinPercentage, "trailMinPercentage");
        Non.negativeOrZero(partitionMaxPercentage, "trailMaxPercentabe");
    }

    public Result create(long total, int count, long tail) {
        long approxTrailSize = Math.round(trailPercentage / 100 * total);
        long approxMainSize = (total - approxTrailSize) / count;

        long percentageMinSize = Math.round(partitionMinPercentage / 100 * approxMainSize);
        long tailAdjustedSize = Math.min(tail * 5, approxMainSize / 10);
        long minSize = tail > 0
            ? tailAdjustedSize
            : percentageMinSize;

        List<Partition> list = partitions(approxTrailSize, minSize);
        Partitions partitions = new Partitions(
            list.stream().mapToLong(Partition::length).sum(),
            list,
            0L
        );
        long trailStart = total - partitions.total();

        return new Result(trailStart, partitions);
    }

    private List<Partition> partitions(long trailSize, long minSize) {
        int blocksCount = (trailCount + 1) * trailCount;
        long blockTotal = trailSize - trailCount * minSize;
        long blockSize = blockTotal / blocksCount;

        List<Partition> list = new ArrayList<>();
        for (int i = 0; i < trailCount; i++) {
            long blocks = (trailCount - i) * blockSize;

            long offset = i == 0
                ? 0L
                : list.getLast().endIndex();
            long size = align(minSize + blocks, ALIGNED);
            list.add(new Partition(
                i,
                trailCount,
                offset,
                size
            ));
        }
        return list;
    }

    private static final int ALIGNED = 16;

    private static long align(long availableRaw, int alignment) {
        return availableRaw - availableRaw % alignment;
    }

    public record Result(long trailStart, Partitions partitions) {
    }
}
