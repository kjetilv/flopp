package com.github.kjetilv.flopp.kernel;

import java.nio.file.Path;
import java.util.function.Supplier;

@SuppressWarnings("unused")
public interface PartitionedPath extends Partitioned<Path> {

    Supplier<Long> lineCounter();

    PartitionedProcessor processor();
}
