package com.github.kjetilv.flopp.kernel;

public record Partitioning(int partitionCount, int bufferSize) {

    public static Partitioning create(int partitionCount, int bufferSize) {
        return new Partitioning(partitionCount, bufferSize);
    }

    public Partitioning {
        Non.negativeOrZero(partitionCount, "partitionCount");
        Non.negativeOrZero(bufferSize, "bufferSize");
    }
}
