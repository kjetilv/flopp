package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.Function;
import java.util.stream.Stream;

@SuppressWarnings("preview")
final class FormatPartitionedProcessor
    extends AbstractPartitionedProcessor<SeparatedLine, Stream<LineSegment>> {

    private final Path target;

    private final Format format;

    FormatPartitionedProcessor(Partitioned partitioned, Path target, Format format) {
        super(partitioned);
        this.target = target;
        this.format = Objects.requireNonNull(format, "format");
    }

    @SuppressWarnings("resource")
    @Override
    public void forEachPartition(Function<Partition, Function<SeparatedLine, Stream<LineSegment>>> processor) {
        LinesWriterFactory<Path, Stream<LineSegment>> writers = path ->
            new LineSegmentsWriter(path, MEMORY_SEGMENT_SIZE);
        ResultCollector<Path> collector = new ResultCollector<>(
            partitioned().partitions().size(),
            path -> Shape.of(path).size(),
            Executors.newVirtualThreadPerTaskExecutor()
        );
        try (
            TempTargets<Path> tempTargets = new TempDirTargets(target);
            Transfers<Path> transfers = new FileChannelTransfers(target);
            StructuredTaskScope<PartitionResult<Path>> scope = new StructuredTaskScope<>()
        ) {
            partitioned().splitters(format)
                .forEach(splitter ->
                    scope.fork(() -> {
                        Path tempTarget = tempTargets.temp(splitter.partition());
                        try (LinesWriter<Stream<LineSegment>> writer = writers.create(tempTarget)) {
                            splitter.separatedLines()
                                .map(processor.apply(splitter.partition()))
                                .forEach(writer);
                        }
                        collector.sync(
                            new PartitionResult<>(
                                splitter.partition(),
                                tempTarget
                            ));
                        return null;
                    }));
            try {
                scope.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted", e);
            }
            collector.syncTo(transfers);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final int BUFFER_SIZE = 8192;

    private static final int MEMORY_SEGMENT_SIZE = 8 * BUFFER_SIZE;
}
