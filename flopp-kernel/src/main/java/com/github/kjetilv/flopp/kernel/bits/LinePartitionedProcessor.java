package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.io.LinesWriter;
import com.github.kjetilv.flopp.kernel.io.LinesWriterFactory;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.Function;

@SuppressWarnings("preview")
public final class LinePartitionedProcessor extends AbstractPartitionedProcessor<LineSegment, String> {

    private final Shape shape;

    public LinePartitionedProcessor(Partitioned<Path> partitioned, Shape shape, Path target) {
        super(partitioned, target);
        this.shape = Objects.requireNonNull(shape, "shape");
    }

    @SuppressWarnings("resource")
    @Override
    public void processFor(Function<Partition, Function<LineSegment, String>> processor) {
        LinesWriterFactory<Path, String> writers = path ->
            new MemoryMappedByteArrayLinesWriter(path, BUFFER_SIZE, shape.charset());
        ResultCollector<Path> collector = new ResultCollector<>(
            partitioned().partitions().size(),
            sizer(),
            Executors.newVirtualThreadPerTaskExecutor()
        );
        try (
            TempTargets<Path> tempTargets = new TempDirTargets(target());
            Transfers<Path> transfers = new FileChannelTransfers(target());
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
