package com.github.kjetilv.flopp.kernel.partitions;

import jdk.incubator.vector.ByteVector;

import static java.lang.foreign.ValueLayout.JAVA_LONG;

public record Partitionings(int alignment) {

    public Partitioning single() {
        return create(1);
    }

    public Partitioning create(int count) {
        return create(count, 0);
    }

    public Partitioning create(int count, long tail) {
        return new Partitioning(
            count,
            tail,
            null,
            alignment
        );
    }

    public Partitioning create() {
        return new Partitioning(CPUS, 0, null, alignment);
    }

    public static final Partitionings LONG = new Partitionings((int) JAVA_LONG.byteAlignment());

    public static final Partitionings BYTE_VECTOR = new Partitionings(ByteVector.SPECIES_PREFERRED.length());

    private static final int CPUS = Runtime.getRuntime().availableProcessors();
}
