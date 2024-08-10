package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.formats.Partitioning;
import com.github.kjetilv.flopp.kernel.formats.Shape;

import java.nio.file.Path;

public final class Bitwise {

    public static Partitioned<Path> partititioned(Path path) {
        return partititioned(path, null, null);
    }

    public static Partitioned<Path> partititioned(Path path, Partitioning partitioning) {
        return partititioned(path, partitioning, null);
    }

    public static Partitioned<Path> partititioned(Path path, Shape shape) {
        return partititioned(path, null, shape);
    }

    public static Partitioned<Path> partititioned(Path path, Partitioning partitioning, Shape shape) {
        return new BitwisePartitioned(path, partitioning, shape);
    }

    private Bitwise() {
    }
}
