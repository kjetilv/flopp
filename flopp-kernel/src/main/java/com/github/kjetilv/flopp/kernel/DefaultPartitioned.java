package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

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
        ToIntFunction<P> sizer,
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
    public PartitionedProcessor<NLine> nLineProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfer,
        ToIntFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    ) {
        return new NLinePartitionProcessor(
            linesWriterFactory,
            tempTargets,
            sizer,
            transfer
        );
    }

    @Override
    public PartitionedProcessor<RNLine> rnLineProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfer,
        ToIntFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    ) {
        return new RNLinePartitionProcessor(
            linesWriterFactory,
            tempTargets,
            sizer,
            transfer
        );
    }

    @Override
    public PartitionedProcessor<byte[]> bytesProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfer,
        ToIntFunction<P> sizer,
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
    public PartitionedProcessor<ByteSegPartitionSpliterator.ByteSeg> segmentProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfer,
        ToIntFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    ) {
        return new ByteSegPartitionProcessor<>(
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

    private final class NLinePartitionProcessor extends AbstractPartitionProcessor<P, NLine> {

        private NLinePartitionProcessor(
            LinesWriterFactory<P> linesWriterFactory,
            TempTargets<P> tempTargets,
            ToIntFunction<P> sizer,
            Transfers<P> transfer
        ) {
            super(
                DefaultPartitioned.this.mapper(),
                DefaultPartitioned.this.shape.charset(),
                DefaultPartitioned.this.partitioning.partitionCount(),
                linesWriterFactory,
                tempTargets,
                sizer,
                transfer,
                DefaultPartitioned.this.executorService
            );
        }

        @Override
        protected Stream<CompletableFuture<PartitionResult<P>>> futures(
            BiFunction<Partition, Stream<NLine>, P> processor,
            PartitionedMapper mapper
        ) {
            return mapper.mapNLines(processor);
        }
    }

    private final class RNLinePartitionProcessor extends AbstractPartitionProcessor<P, RNLine> {

        private RNLinePartitionProcessor(
            LinesWriterFactory<P> linesWriterFactory,
            TempTargets<P> tempTargets,
            ToIntFunction<P> sizer,
            Transfers<P> transfer
        ) {
            super(
                DefaultPartitioned.this.mapper(),
                DefaultPartitioned.this.shape.charset(),
                DefaultPartitioned.this.partitioning.partitionCount(),
                linesWriterFactory,
                tempTargets,
                sizer,
                transfer,
                DefaultPartitioned.this.executorService
            );
        }

        @Override
        protected Stream<CompletableFuture<PartitionResult<P>>> futures(
            BiFunction<Partition, Stream<RNLine>, P> processor,
            PartitionedMapper mapper
        ) {
            return mapper.mapRNLines(processor);
        }
    }
}
