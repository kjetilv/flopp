package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.PartitionedProcessor;

import java.nio.file.Path;
import java.util.function.Supplier;

@SuppressWarnings("unused")
public interface PartitionedPath extends Partitioned<Path> {

    Supplier<Long> lineCounter();

    PartitionedProcessor processor();
}
