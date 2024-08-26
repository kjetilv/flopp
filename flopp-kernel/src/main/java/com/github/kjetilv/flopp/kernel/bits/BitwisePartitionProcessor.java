package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.formats.Shape;
import com.github.kjetilv.flopp.kernel.io.LinesWriter;
import com.github.kjetilv.flopp.kernel.io.LinesWriterFactory;
import com.github.kjetilv.flopp.kernel.io.TempTargets;
import com.github.kjetilv.flopp.kernel.io.Transfers;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

final class BitwisePartitionProcessor implements PartitionedProcessor<LineSegment> {

    private final PartitionedMapper partitionedMapper;

    private final LinesWriterFactory<Path> linesWriterFactory;

    private final TempTargets<Path> tempTargets;

    private final Transfers<Path> transfers;

    private final Partitions partitions;

    private final Charset charset;

    BitwisePartitionProcessor(
        PartitionedMapper partitionedMapper,
        Partitions partitions,
        Charset charset,
        LinesWriterFactory<Path> linesWriterFactory,
        TempTargets<Path> tempTargets,
        Transfers<Path> transfers
    ) {
        this.partitionedMapper = Objects.requireNonNull(partitionedMapper, "partitionedMapper");
        this.linesWriterFactory = Objects.requireNonNull(linesWriterFactory, "linesWriterFactory");
        this.tempTargets = Objects.requireNonNull(tempTargets, "tempTargets");
        this.transfers = Objects.requireNonNull(transfers, "transfers");
        this.partitions = partitions;
        this.charset = charset;
    }

    @Override
    public void process(Function<LineSegment, String> processor, ExecutorService executorService) {
        ResultCollector<Path> collector =
            new ResultCollector<>(partitions.size(), path -> Shape.of(path, charset).size());
        CompletableFuture<Void> streamFuture = CompletableFuture.runAsync(
            () -> {
                BiFunction<Partition, Stream<LineSegment>, Path> processing =
                    (partition, lines) -> {
                        Path tempTarget = tempTargets.temp(partition);
                        try (LinesWriter linesWriter = linesWriterFactory.create(tempTarget, charset)) {
                            lines.forEach(line ->
                                linesWriter.accept(processor.apply(line)));
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to write " + tempTarget, e);
                        }
                        return tempTarget;
                    };
                futures(
                    processing,
                    partitionedMapper,
                    executorService
                ).forEach(collector::collect);
            },
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

    @Override
    public void close() {
        transfers.close();
    }

    private static Stream<CompletableFuture<PartitionResult<Path>>> futures(
        BiFunction<Partition, Stream<LineSegment>, Path> processor,
        PartitionedMapper mapper,
        ExecutorService executorService
    ) {
        return mapper.map(processor, executorService);
    }
}
