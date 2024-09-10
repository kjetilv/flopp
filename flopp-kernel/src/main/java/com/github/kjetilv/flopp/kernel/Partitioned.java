package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.formats.CsvFormat;
import com.github.kjetilv.flopp.kernel.formats.FwFormat;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface Partitioned<P> extends Closeable {

    P partitioned();

    Partitions partitions();

    PartitionedProcessor<LineSegment, String> processor(Path target);

    PartitionedProcessor<SeparatedLine, Stream<LineSegment>> processor(Path target, CsvFormat format);

    PartitionedMapper<LineSegment> mapper();

    PartitionedConsumer consumer();

    PartitionedSplitters splitters();

    default Stream<PartitionedSplitter> splitters(CsvFormat format) {
        return splitters().splitters(format);
    }

    default Stream<CompletableFuture<PartitionedSplitter>> splitters(
        CsvFormat format,
        ExecutorService executorService
    ) {
        return splitters().splitters(format, executorService);
    }

    default Stream<PartitionedSplitter> splitters(FwFormat format) {
        return splitters().splitters(format);
    }

    default Stream<CompletableFuture<PartitionedSplitter>> splitters(FwFormat format, ExecutorService executorService) {
        return splitters().splitters(format, executorService);
    }

    PartitionedStreams streams();

    @Override
    void close();

    private static <T> List<T> awaitCompleted(Stream<CompletableFuture<T>> futures) {
        return futures
            .toList()
            .stream()
            .map(CompletableFuture::join)
            .toList();
    }
}
