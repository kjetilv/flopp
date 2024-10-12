package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.partitions.Partitions;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface Partitioned<P> extends Closeable {

    P partitioned();

    Partitions partitions();

    Stream<LongSupplier> lineCounters();

    Stream<PartitionStreamer> streamers();

    Stream<PartitionedSplitter> splitters(Format format);

    PartitionedProcessor<Path, LineSegment, String> processTo(P target, Charset charSet);

    PartitionedProcessor<P, SeparatedLine, Stream<LineSegment>> processTo(P target, Format format);

    @Override
    void close();
}
