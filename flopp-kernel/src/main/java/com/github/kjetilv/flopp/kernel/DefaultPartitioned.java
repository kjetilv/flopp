package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.ToLongFunction;

public class DefaultPartitioned<P> implements Partitioned<P> {

    private final P path;

    private final Shape shape;

    private final Partitioning partitioning;

    private final ExecutorService executorService;

    private final ByteSources sources;

    public DefaultPartitioned(
        P path,
        Shape shape,
        Partitioning partitioning,
        ByteSources sources,
        ExecutorService executorService
    ) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.executorService = Objects.requireNonNull(executorService, "executorService");
        this.sources = Objects.requireNonNull(sources, "sources");
    }

    @Override
    public P partitioned() {
        return path;
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
    public PartitionedProcessor<String> processor(
        TempTargets<P> tempTargets,
        Transfers<P> transfer,
        ToLongFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    ) {
        return new StringPartitionProcessor<>(
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
    public PartitionedProcessor<byte[]> bytesProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfer,
        ToLongFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    ) {
        return new BytesPartitionProcessor<>(
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

    protected ExecutorService executorService() {
        return executorService;
    }

    protected Partitioning partitioning() {
        return partitioning;
    }

    protected Shape shape() {
        return shape;
    }
}
