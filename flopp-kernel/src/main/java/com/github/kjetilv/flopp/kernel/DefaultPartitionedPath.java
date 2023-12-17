package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.files.FileChannelTransfers;
import com.github.kjetilv.flopp.kernel.files.FileTempTargets;
import com.github.kjetilv.flopp.kernel.files.MemoryMappedByteArrayLinesWriter;
import com.github.kjetilv.flopp.kernel.lc.AsyncLineCounter;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class DefaultPartitionedPath extends DefaultPartitioned<Path>
    implements PartitionedPath {

    private final AtomicLong lineCount = new AtomicLong(-1);

    DefaultPartitionedPath(
        Path path,
        Shape shape,
        Partitioning partitioning,
        ByteSources sources,
        ExecutorService executorService
    ) {
        super(path, shape, partitioning, sources, executorService);
    }

    @Override
    public PartitionedProcessor processor() {
        return processor(
            new FileTempTargets(partitioned()),
            new FileChannelTransfers(partitioned()),
            PartitionedPaths::sizeOf,
            (target, charset) ->
                new MemoryMappedByteArrayLinesWriter(
                    target,
                    partitioning().bufferSize(),
                    charset
                ));
    }

    @Override
    public Supplier<Long> lineCounter() {
        return () ->
            lineCount.updateAndGet(alreadyComputed ->
                alreadyComputed < 0
                    ? count()
                    : alreadyComputed);
    }

    private long count() {
        return new AsyncLineCounter(
            executorService(),
            partitioning().bufferSize()
        ).count(
            partitioned(),
            shape()
        );
    }
}
