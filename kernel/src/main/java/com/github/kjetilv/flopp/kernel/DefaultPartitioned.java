package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.ToLongFunction;

class DefaultPartitioned<P> implements Partitioned<P> {

    private final Shape shape;

    private final Partitioning partitioning;

    private final ExecutorService executorService;

    private final ByteSources sources;

    DefaultPartitioned(
        Shape shape,
        Partitioning partitioning,
        ByteSources sources,
        ExecutorService executorService
    ) {
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.executorService = Objects.requireNonNull(executorService, "executorService");
        this.sources = Objects.requireNonNull(sources, "sources");
    }

    @Override
    public PartitionedStreams streams() {
        return new DefaultPartitionedStreams(shape, partitioning, sources);
    }

    @Override
    public PartitionedMapper mapper() {
        return new DefaultPartitionedMapper(streams(), sources, executorService);
    }

    @Override
    public PartitionedConsumer consumer() {
        return new DefaultPartitionedConsumer(mapper(), sources);
    }

    @Override
    public PartitionedProcessor processor(
        TempTargets<P> tempTargets,
        Transfers<P> transfer,
        ToLongFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    ) {
        return new DefaultPartitionProcessor<>(
            mapper(),
            shape.charset(),
            partitioning.partitionCount(),
            linesWriterFactory,
            tempTargets,
            sizer,
            transfer,
            executorService
        );
    }

    @Override
    public void close() {
        sources.close();
    }
}
