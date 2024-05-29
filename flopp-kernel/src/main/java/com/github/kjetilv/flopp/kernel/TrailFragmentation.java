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

        long minSize = Math.max(
            tail * 2,
            Math.round(partitionMinPercentage / 100 * approxMainSize)
        );

        int blocksCount = (trailCount + 1) * trailCount;
        long blockTotal = approxTrailSize - trailCount * minSize;
        long blockSize = blockTotal / blocksCount;

        List<Partition> list = new ArrayList<>();
        for (int i = 0; i < trailCount; i++) {
            long blocks = (trailCount - i) * blockSize;

            long offset = i == 0
                ? 0L
                : list.getLast().endIndex();
            long size = align(minSize + blocks, ALIGNMENT);
            list.add(new Partition(
                i,
                trailCount,
                offset,
                size
            ));
        }
        Partitions partitions = new Partitions(
            list.stream().mapToLong(Partition::length).sum(),
            list,
            0L
        );
        long trailStart = total - partitions.total();

        return new Result(trailStart, partitions);
    }

    private static final int ALIGNMENT = 16;

    private static long align(long availableRaw, int alignment) {
        return availableRaw - availableRaw % alignment;
    }

    public record Result(long trailStart, Partitions partitions) {
    }
}
