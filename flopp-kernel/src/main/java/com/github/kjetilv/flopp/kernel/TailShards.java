package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.util.Non;

import java.util.ArrayList;
import java.util.List;

public record TailShards(
    int shardCount,
    int tailDim,
    int maxDim,
    int minDim
) {

    public TailShards {
        if (tailDim == 0 && maxDim == 0 && minDim == 0) {
        } else if (tailDim < maxDim && maxDim < minDim) {
            Non.negativeOrZero(shardCount, "shardCount");
            Non.negativeOrZero(tailDim, "tailDim");
            Non.negativeOrZero(minDim, "minDim");
            Non.negativeOrZero(maxDim, "maxDim");
        } else {
            throw new IllegalStateException(this + " has wrong sizes");
        }
    }

    public Partitions create(long total, int count, long tail) {
        long approxTailSize = Math.round(total / Math.pow(10, tailDim));
        long approxMainSize = (total - approxTailSize) / count;

        long minSize = tail > 0
            ? Math.min(tail * 5, approxMainSize / 10)
            : Math.round(approxMainSize / Math.pow(10, minDim));

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
