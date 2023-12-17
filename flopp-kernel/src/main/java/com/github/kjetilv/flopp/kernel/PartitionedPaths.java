package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.files.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

@SuppressWarnings("unused")
public final class PartitionedPaths {

    public static Partitioned<Path> create(
        Path path,
        Partitioning partitioning
    ) {
        return create(
            path,
            null,
            partitioning
        );
    }

    public static Partitioned<Path> create(
        Path path,
        Shape shape,
        Partitioning partitioning
    ) {
        return create(
            path,
            shape,
            partitioning,
            null
        );
    }

    public static Partitioned<Path> create(
        Path path,
        Partitioning partitioning,
        ExecutorService executorService
    ) {
        return create(
            path,
            null,
            partitioning,
            executorService
        );
    }

    public static Partitioned<Path> create(
        Path path,
        Shape shape,
        Partitioning partitioning,
        ExecutorService executorService
    ) {
        Shape fileShape = shape == null ? Shape.size(sizeOf(path)) : shape;
        return new DefaultPartitioned<>(
            fileShape,
            partitioning,
            new FileChannelSources(path, fileShape.size()),
            executorService == null
                ? ForkJoinPool.commonPool()
                : executorService
        );
    }

    public static PartitionedProcessor processor(
        Path path,
        Shape shape,
        Partitioning partitioning,
        Path target
    ) {
        return processor(
            path,
            shape,
            partitioning,
            target,
            null
        );
    }

    public static PartitionedProcessor processor(
        Path path,
        Shape shape,
        Partitioning partitioning,
        Path target,
        ExecutorService executorService
    ) {
        return create(
            path,
            shape,
            partitioning,
            executorService
        ).processor(
            new FileTempTargets(target),
            new FileChannelTransfers(target),
            PartitionedPaths::sizeOf,
            (p, charset) ->
                new MemoryMappedByteArrayLinesWriter(p, partitioning.bufferSize(), charset)

        );
    }

    private static long sizeOf(Path path) {
        try {
            return Files.size(path);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to size " + path, e);
        }
    }

    private PartitionedPaths() {
    }
}
