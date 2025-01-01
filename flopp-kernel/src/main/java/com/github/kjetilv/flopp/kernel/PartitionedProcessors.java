package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.stream.Stream;

public interface PartitionedProcessors<P> extends Closeable {

    @Override
    default void close() {
    }

    PartitionedProcessor<LineSegment, String> processTo(
        P target,
        Charset charSet
    );

    PartitionedProcessor<SeparatedLine, Stream<LineSegment>> processTo(
        P target,
        Format format
    );
}
