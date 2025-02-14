package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.Shape;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.Function;

@SuppressWarnings("preview")
final class LinePartitionedProcessor extends AbstractPartitionedProcessor<LineSegment, String> {

    private final Path target;

    private final Charset charset;

    LinePartitionedProcessor(Partitioned partitioned, Path target, Charset charset) {
        super(partitioned);
        this.target = target;
        this.charset = Objects.requireNonNull(charset, "charset");
    }

    @SuppressWarnings("resource")
    @Override
    public void forEachPartition(Function<Partition, Function<LineSegment, String>> processor) {
        LinesWriterFactory<Path, String> writers = path ->
            new MemoryMappedByteArrayLinesWriter(path, BUFFER_SIZE, charset);
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
            partitioned().streamers()
                .forEach(streamer ->
                    scope.fork(() -> {
                        Path tempTarget = tempTargets.temp(streamer.partition());
                        try (LinesWriter<String> writer = writers.create(tempTarget)) {
                            streamer.lines()
                                .map(processor.apply(streamer.partition()))
                                .forEach(writer);
                        }
                        collector.sync(new PartitionResult<>(streamer.partition(), tempTarget));
                        return null;
                    }));
            join(scope);
            collector.syncTo(transfers);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final int BUFFER_SIZE = 8192;
}
