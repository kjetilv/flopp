package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.*;
import java.util.stream.Stream;

public abstract class AbstractPartitionProcessor<P, T> implements PartitionedProcessor<T> {

    private final PartitionedMapper partitionedMapper;

    private final Charset charset;

    private final int partitionCount;

    private final LinesWriterFactory<P> linesWriterFactory;

    private final TempTargets<P> tempTargets;

    private final ToIntFunction<P> sizer;

    private final Transfers<P> transfers;

    private final ExecutorService executorService;

    public AbstractPartitionProcessor(
        PartitionedMapper partitionedMapper,
        Charset charset,
        int partitionCount,
        LinesWriterFactory<P> linesWriterFactory,
        TempTargets<P> tempTargets,
        ToIntFunction<P> sizer,
        Transfers<P> transfers,
        ExecutorService executorService
    ) {
        this.partitionedMapper = Objects.requireNonNull(partitionedMapper, "partitionedMapper");
        this.charset = charset == null ? StandardCharsets.UTF_8 : charset;
        this.partitionCount = Non.negativeOrZero(partitionCount, "partitionCount");
        this.linesWriterFactory = Objects.requireNonNull(linesWriterFactory, "linesWriterFactory");
        this.tempTargets = Objects.requireNonNull(tempTargets, "tempTargets");
        this.sizer = Objects.requireNonNull(sizer, "sizer");
        this.transfers = Objects.requireNonNull(transfers, "transfers");
        this.executorService = Objects.requireNonNull(executorService, "executorService");
    }

    @Override
    public void process(Function<T, String> processor) {
        collect(
            new ResultCollector<>(partitionCount, sizer),
            (partition, nLines) ->
                stream(partition, nLines, (consumer, line) ->
                    consumer.accept(processor.apply(line)))
        );
    }

    @Override
    public void processMulti(Function<T, Stream<String>> processor) {
        collect(
            new ResultCollector<>(partitionCount, sizer),
            (partition, nLines) ->
                stream(partition, nLines, (consumer, line) ->
                    processor.apply(line)
                        .forEach(consumer))
        );
    }

    @Override
    public void close() {
        partitionedMapper.close();
        transfers.close();
    }

    void collect(ResultCollector<P> collector, BiFunction<Partition, Stream<T>, P> streamProcessor) {
        CompletableFuture<Void> streamFuture = CompletableFuture.runAsync(
            () ->
                futures(streamProcessor, partitionedMapper).forEach(collector::collect),
            executorService
        );
        List<CompletableFuture<Void>> transferFutures = collector.streamCollected()
            .map(result ->
                transfers.transfer(result.partition(), result.result()))
            .map(transfer ->
                CompletableFuture.runAsync(transfer, executorService))
            .toList();
        try {
            transferFutures.forEach(CompletableFuture::join);
        } finally {
            streamFuture.join();
        }
    }

    protected abstract Stream<CompletableFuture<PartitionResult<P>>> futures(
        BiFunction<Partition, Stream<T>, P> processor,
        PartitionedMapper mapper
    );

    private P stream(Partition partition, Stream<T> ts, BiConsumer<Consumer<String>, T> fun) {
        P target = tempTargets.temp(partition);
        try (LinesWriter linesWriter = linesWriterFactory.create(target, charset)) {
            ts.forEach(feed(linesWriter, fun));
        } catch (Exception e) {
            throw new RuntimeException("Failed to write " + target, e);
        }
        return target;
    }

    private Consumer<T> feed(Consumer<String> linesWriter, BiConsumer<Consumer<String>, T> fun) {
        return t -> fun.accept(linesWriter, t);
    }
}
