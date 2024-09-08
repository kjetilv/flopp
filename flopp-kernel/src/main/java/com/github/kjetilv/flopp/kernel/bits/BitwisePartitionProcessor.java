package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.io.LinesWriter;
import com.github.kjetilv.flopp.kernel.io.LinesWriterFactory;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

final class BitwisePartitionProcessor<I, O> implements PartitionedProcessor<I, O> {

    private final PartitionedMapper<I> partitionedMapper;

    private final LinesWriterFactory<Path, O> linesWriterFactory;

    private final TempTargets<Path> tempTargets;

    private final Transfers<Path> transfers;

    private final Partitions partitions;

    BitwisePartitionProcessor(
        PartitionedMapper<I> partitionedMapper,
        Partitions partitions,
        LinesWriterFactory<Path, O> linesWriterFactory,
        TempTargets<Path> tempTargets,
        Transfers<Path> transfers
    ) {
        this.partitionedMapper = Objects.requireNonNull(partitionedMapper, "partitionedMapper");
        this.linesWriterFactory = Objects.requireNonNull(linesWriterFactory, "linesWriterFactory");
        this.tempTargets = Objects.requireNonNull(tempTargets, "tempTargets");
        this.transfers = Objects.requireNonNull(transfers, "transfers");
        this.partitions = Objects.requireNonNull(partitions, "partitions");
    }

    @Override
    public void process(Function<I, O> processor, ExecutorService executorService) {
        ResultCollector<Path> collector =
            new ResultCollector<>(partitions.size(), path -> Shape.of(path).size());
        CompletableFuture<Void> streamFuture = CompletableFuture.runAsync(
            () -> {
                BiFunction<Partition, Stream<I>, Path> processing =
                    (partition, lines) -> {
                        Path tempTarget = tempTargets.temp(partition);
                        try (LinesWriter<O> linesWriter = linesWriterFactory.create(tempTarget)) {
                            lines.forEach(line ->
                                linesWriter.accept(processor.apply(line)));
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to write " + tempTarget, e);
                        }
                        return tempTarget;
                    };
                partitionedMapper.map(processing, executorService)
                    .forEach(collector::collect);
            },
            executorService
        );
        try {
            collector.streamCollected()
                .map(result ->
                    transfers.transfer(result.partition(), result.result()).in(executorService))
                .toList()
                .forEach(CompletableFuture::join);
        } finally {
            streamFuture.join();
        }
    }

    @Override
    public void close() {
        transfers.close();
    }
}
