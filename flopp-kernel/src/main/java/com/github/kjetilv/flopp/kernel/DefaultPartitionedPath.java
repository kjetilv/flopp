package com.github.kjetilv.flopp.kernel;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

public class DefaultPartitionedPath extends DefaultPartitioned<Path>
    implements PartitionedPath {

    DefaultPartitionedPath(
        Path path,
        Shape shape,
        Partitioning partitioning,
        ByteSources sources,
        ExecutorService executorService
    ) {
        super(path, shape, partitioning, sources, executorService);
    }
}
