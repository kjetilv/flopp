package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Shape;

import java.nio.file.Path;

public final class Bitwise {

    public static Partitioned<Path> partitioned(Path path) {
        return partitioned(path, null, null);
    }

    public static Partitioned<Path> partitioned(Path path, Partitioning partitioning) {
        return partitioned(path, partitioning, null);
    }

    public static Partitioned<Path> partitioned(Path path, Shape shape) {
        return partitioned(path, null, shape);
    }

    public static Partitioned<Path> partitioned(Path path, Partitioning partitioning, Shape shape) {
        return new BitwisePartitioned(path, partitioning, shape);
    }

    private Bitwise() {
    }
}
