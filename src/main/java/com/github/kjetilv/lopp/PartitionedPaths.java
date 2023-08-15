package com.github.kjetilv.lopp;

import com.github.kjetilv.lopp.files.FileChannelSources;
import com.github.kjetilv.lopp.files.FileChannelTransfers;
import com.github.kjetilv.lopp.files.FileTempTargets;
import com.github.kjetilv.lopp.files.MemoryMappedByteArrayLinesWriter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

@SuppressWarnings("unused")
public final class PartitionedPaths {

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
        Shape shape,
        Partitioning partitioning,
        ExecutorService executorService
    ) {
        return new DefaultPartitioned<>(
            shape,
            partitioning,
            new FileChannelSources(path, shape.size()),
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
            p -> {
                try {
                    return Files.size(p);
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to size " + p, e);
                }
            },
            (p, charset) ->
                new MemoryMappedByteArrayLinesWriter(p, partitioning.bufferSize(), charset)

        );
    }

    private PartitionedPaths() {
    }
}
