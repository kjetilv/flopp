package com.github.kjetilv.flopp.kernel;

public record Partitioning(int partitionCount, int bufferSize) {

    public static Partitioning create(int partitionCount, int bufferSize) {
        return new Partitioning(partitionCount, bufferSize);
    }

    public static Partitioning defaults() {
        return defaults(DEFAULT_BUFFER);
    }

    public static Partitioning defaults(int bufferSize) {
        return new Partitioning(Runtime.getRuntime().availableProcessors(), bufferSize);
    }

    public Partitioning {
        Non.negativeOrZero(partitionCount, "partitionCount");
        Non.negativeOrZero(bufferSize, "bufferSize");
    }

    public int bufferSizeOr(int defaultSize) {
        return bufferSize > 0 ? bufferSize : defaultSize;
    }

    public static final int DEFAULT_BUFFER = 16 * 1024;
}
