package com.github.kjetilv.flopp.kernel;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

public class DefaultPartitioned<P> implements Partitioned<P> {

    private final P path;


    private final Shape shape;

    private final Partitioning partitioning;

    private final Sources sources;

    private final ExecutorService executorService;

    public DefaultPartitioned(
        P path,
        Shape shape,
        Partitioning partitioning,
        Sources sources,
        ExecutorService executorService
    ) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.sources = Objects.requireNonNull(sources, "sources");
        this.executorService = Objects.requireNonNull(executorService, "executorService");
    }

    @Override
    public P partitioned() {
        return path;
    }

    @Override
    public List<Partition> partitions() {
        return partitioning.of(shape.size());
    }

    @Override
    public PartitionedStreams streams() {
        return new DefaultPartitionedStreams(shape, partitioning, sources);
    }

    @Override
    public VectorPartitionedMapper vectorMapper() {
        return new DefaultVectorPartitionedMapper(streams(), executorService);
    }

    @Override
    public PartitionedMapper mapper() {
        return new DefaultPartitionedMapper(streams(), executorService);
    }

    @Override
    public PartitionedConsumer consumer() {
        return new DefaultPartitionedConsumer(mapper());
    }

    @Override
    public PartitionedProcessor<String> processor(
        TempTargets<P> tempTargets,
        Transfers<P> transfers,
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
            transfers,
            executorService
        );
    }

    @Override
    public PartitionedProcessor<byte[]> bytesProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfers,
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
            transfers,
            executorService
        );
    }

    @Override
    public PartitionedProcessor<NLine> nLineProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfers,
        ToIntFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    ) {
        return new NLinePartitionProcessor(
            linesWriterFactory,
            tempTargets,
            sizer,
            transfers
        );
    }

    @Override
    public PartitionedProcessor<RNLine> rnLineProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfers,
        ToIntFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    ) {
        return new RNLinePartitionProcessor(
            linesWriterFactory,
            tempTargets,
            sizer,
            transfers
        );
    }

    @Override
    public PartitionedProcessor<ByteSeg> byteSegProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfers,
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
            transfers,
            executorService
        );
    }

    @Override
    public PartitionedProcessor<Supplier<ByteSeg>> suppliedByteSegProcessor(
        TempTargets<P> tempTargets,
        Transfers<P> transfers,
        ToIntFunction<P> sizer,
        LinesWriterFactory<P> linesWriterFactory
    ) {
        return new ByteSegSupPartitionProcessor<>(
            mapper(),
            shape.charset(),
            partitioning.partitionCount(),
            linesWriterFactory,
            tempTargets,
            sizer,
            transfers,
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
