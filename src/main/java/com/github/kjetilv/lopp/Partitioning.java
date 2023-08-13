package com.github.kjetilv.lopp;

import com.github.kjetilv.lopp.utils.Non;

public record Partitioning(
    int partitionCount,
    int bufferSize,
    int scanResolution
) {

    public static Partitioning create(int partitionCount, int bufferSize) {
        return new Partitioning(partitionCount, bufferSize, 1);
    }

    public Partitioning {
        Non.negativeOrZero(partitionCount, "partitionCount");
        Non.negativeOrZero(bufferSize, "bufferSize");
        Non.negativeOrZero(scanResolution,"scanResolution");
    }
}
