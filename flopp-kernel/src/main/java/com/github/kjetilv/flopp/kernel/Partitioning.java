package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.ValueLayout;
import java.util.List;

@SuppressWarnings("unused")
public record Partitioning(
    int partitionCount,
    int alignment,
    int bufferSize
) {
    public Partitioning(int partitionCount, int bufferSize) {
        this(partitionCount, 1, bufferSize);
    }

    public static Partitioning longAlignedDefaults() {
        return longAlignedDefaults(0);
    }

    public static Partitioning longAlignedDefaults(int cpus) {
        return new Partitioning(
            cpus > 0 ? cpus : cpus(),
            Math.toIntExact(ValueLayout.JAVA_LONG.byteSize()),
            DEFAULT_BUFFER
        );
    }

    public static Partitioning defaults() {
        return defaults(DEFAULT_BUFFER);
    }

    public static Partitioning defaults(int bufferSize) {
        return new Partitioning(cpus(), 1, bufferSize);
    }

    public Partitioning {
        Non.negativeOrZero(partitionCount, "partitionCount");
        Non.negativeOrZero(alignment,"alignment");
        Non.negativeOrZero(bufferSize, "bufferSize");
    }

    public List<Partition> of(long total) {
        return Partition.partitions(total, partitionCount, alignment);
    }

    public int bufferSizeOr(int defaultSize) {
        return bufferSize > 0 ? bufferSize : defaultSize;
    }

    public static final int DEFAULT_BUFFER = 16 * 1024;

    private static int cpus() {
        return Runtime.getRuntime().availableProcessors();
    }
}
