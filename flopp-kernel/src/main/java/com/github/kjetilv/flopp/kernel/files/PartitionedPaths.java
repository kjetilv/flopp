package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

@SuppressWarnings("unused")
public final class PartitionedPaths {

    public static PartitionedPath create(
        Path path,
        Partitioning partitioning
    ) {
        return create(
            path,
            null,
            partitioning
        );
    }

    public static PartitionedPath create(
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

    public static PartitionedPath create(
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

    public static PartitionedPath create(
        Path path,
        Shape shape,
        Partitioning partitioning,
        ExecutorService executorService
    ) {
        Shape fileShape = resolveShape(path, shape);
        return new DefaultPartitionedPath(
            path,
            fileShape,
            partitioning,
            new FileChannelSources(path, fileShape.size(), PADDING),
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
            resolveShape(path, shape),
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

    private static final int PADDING = 1024;

    private static Shape resolveShape(Path path, Shape shape) {
        return shape == null
            ? Shape.size(sizeOf(path))
            : shape.sized(() -> sizeOf(path));
    }

    static long sizeOf(Path path) {
        try {
            return Files.size(path);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to size " + path, e);
        }
    }

    private PartitionedPaths() {
    }
}
