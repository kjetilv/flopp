package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.PartitionedProcessor;

import java.nio.file.Path;

@SuppressWarnings("unused")
public interface PartitionedPath extends Partitioned<Path> {

    PartitionedProcessor<String> processor();

    long lineCount();
}
