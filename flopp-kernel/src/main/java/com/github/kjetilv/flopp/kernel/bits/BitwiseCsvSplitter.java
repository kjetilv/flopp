package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

final class BitwiseCsvSplitter implements PartitionedSplitter {

    private final PartitionStreamer streamer;

    private final CsvFormat format;

    private final boolean immutable;

    BitwiseCsvSplitter(PartitionStreamer streamer, CsvFormat format, boolean immutable) {
        this.streamer = Objects.requireNonNull(streamer, "streamer");
        this.format = Objects.requireNonNull(format, "format");
        this.immutable = immutable;
    }

    @Override
    public Partition partition() {
        return streamer.partition();
    }

    @Override
    public void forEach(Consumer<SeparatedLine> consumer) {
        streamer.lines().forEach(splitter(consumer));
    }

    @Override
    public Stream<SeparatedLine> separatedLine() {
        return streamer.lines().map(splitter(null));
    }

    private AbstractBitwiseLineSplitter splitter(Consumer<SeparatedLine> consumer) {
        return switch (format) {
            case CsvFormat.Escaped escaped -> new BitwiseCsvEscapedLineSplitter(
                consumer,
                escaped,
                immutable
            );
            case CsvFormat.DoubleQuoted doubleQuoted -> new BitwiseCsvDoubleQuotedLineSplitter(
                consumer,
                doubleQuoted,
                immutable
            );
            case CsvFormat.Simple simple -> new BitwiseCsvSimpleLineSplitter(
                consumer,
                simple,
                immutable
            );
        };
    }
}
