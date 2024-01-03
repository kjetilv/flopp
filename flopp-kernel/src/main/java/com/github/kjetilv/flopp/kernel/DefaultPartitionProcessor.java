package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.*;
import java.util.stream.Stream;

class DefaultPartitionProcessor<P> implements PartitionedProcessor {

    private final PartitionedMapper partitionedMapper;

    private final Charset charset;

    private final int partitionCount;

    private final LinesWriterFactory<P> linesWriterFactory;

    private final TempTargets<P> tempTargets;

    private final ToLongFunction<P> sizer;

    private final Transfers<P> transfers;

    private final ExecutorService executorService;

    DefaultPartitionProcessor(
        PartitionedMapper partitionedMapper,
        Charset charset,
        int partitionCount,
        LinesWriterFactory<P> linesWriterFactory,
        TempTargets<P> tempTargets,
        ToLongFunction<P> sizer,
        Transfers<P> transfers,
        ExecutorService executorService
    ) {
        this.partitionedMapper = partitionedMapper;
        this.charset = charset;
        this.partitionCount = partitionCount;
        this.linesWriterFactory = Objects.requireNonNull(linesWriterFactory, "linesWriterFactory");
        this.tempTargets = Objects.requireNonNull(tempTargets, "tempTargets");
        this.sizer = Objects.requireNonNull(sizer, "sizer");
        this.transfers = Objects.requireNonNull(transfers, "transfers");
        this.executorService = Objects.requireNonNull(executorService, "executorService");
    }

    @Override
    public void process(Function<String, String> processor) {
        collect(
            new ResultCollector<P>(partitionCount, sizer),
            (partition, nLines) ->
                stream(partition, nLines, (consumer, npl) ->
                    consumer.accept(processor.apply(npl.line())))
        );
    }

    @Override
    public void processMulti(Function<String, Stream<String>> processor) {
        collect(
            new ResultCollector<P>(partitionCount, sizer),
            (partition, nLines) ->
                stream(partition, nLines, (consumer, npl) ->
                    processor.apply(npl.line())
                        .forEach(consumer))
        );
    }

    @Override
    public void close() {
        partitionedMapper.close();
    }

    private void collect(
        ResultCollector<P> collector, BiFunction<Partition, Stream<NLine>, P> partitionStreamPBiFunction
    ) {
        CompletableFuture<Void> streamFuture = CompletableFuture.runAsync(
            () ->
                partitionedMapper.map(partitionStreamPBiFunction)
                    .forEach(collector::collect),
            executorService
        );
        List<CompletableFuture<Void>> transferFutures = collector.streamCollected()
            .map(partitionResult ->
                transfers.transfer(partitionResult.partition(), partitionResult.result()))
            .map(runnable ->
                CompletableFuture.runAsync(runnable, executorService))
            .toList();
        try {
            transferFutures.forEach(CompletableFuture::join);
        } finally {
            streamFuture.join();
        }
    }

    private P stream(Partition partition, Stream<NLine> nLines, BiConsumer<Consumer<String>, NLine> fun) {
        P target = tempTargets.temp(partition);
        try (LinesWriter linesWriter = linesWriterFactory.create(target, charset)) {
            nLines.forEach(feed(linesWriter, fun));
        } catch (Exception e) {
            throw new RuntimeException("Failed to write " + target, e);
        }
        return target;
    }

    private static Consumer<NLine> feed(Consumer<String> linesWriter, BiConsumer<Consumer<String>, NLine> fun) {
        return nLine -> fun.accept(linesWriter, nLine);
    }
}
