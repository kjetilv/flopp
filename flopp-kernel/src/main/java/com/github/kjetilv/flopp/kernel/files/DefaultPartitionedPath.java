package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.lc.AsyncLineCounter;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

class DefaultPartitionedPath extends DefaultPartitioned<Path>
    implements PartitionedPath {

    private final AtomicLong lineCount = new AtomicLong(-1);

    DefaultPartitionedPath(
        Path path,
        Shape shape,
        Partitioning partitioning,
        Sources sources,
        ExecutorService executorService
    ) {
        super(
            path,
            shape,
            partitioning,
            sources,
            executorService
        );
    }

    @Override
    public PartitionedProcessor<String> processor() {
        return processor(targets(), transfers(), PartitionedPaths::sizeOf, this::writer);
    }

    @Override
    public long lineCount() {
        return lineCount.updateAndGet(alreadyComputed ->
            alreadyComputed < 0
                ? count()
                : alreadyComputed);
    }

    private TempTargets<Path> targets() {
        return new FileTempTargets(partitioned());
    }

    private Transfers<Path> transfers() {
        return new FileChannelTransfers(partitioned());
    }

    private MemoryMappedByteArrayLinesWriter writer(Path target, Charset charset) {
        return new MemoryMappedByteArrayLinesWriter(target, partitioning().bufferSize(), charset);
    }

    private long count() {
        return new AsyncLineCounter(executorService(), partitioning().bufferSize())
            .count(partitioned(), shape());
    }
}
