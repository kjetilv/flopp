package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.CsvFormat.Escape;
import com.github.kjetilv.flopp.kernel.CsvFormat.Quoted;
import com.github.kjetilv.flopp.kernel.CsvFormat.Simple;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

final class BitwiseCsvSplitter implements PartitionedSplitter {

    private final PartitionStreamer streamer;

    private final CsvFormat format;

    BitwiseCsvSplitter(PartitionStreamer streamer, CsvFormat format) {
        this.streamer = Objects.requireNonNull(streamer, "streamer");
        this.format = Objects.requireNonNull(format, "format");
    }

    @Override
    public Partition partition() {
        return streamer.partition();
    }

    @Override
    public void forEach(Consumer<SeparatedLine> consumer) {
        streamer.lines()
            .forEach(splitter(consumer));
    }

    @Override
    public Stream<SeparatedLine> separatedLines() {
        return streamer.lines().map(splitter(null));
    }

    private LineSplitter splitter(Consumer<SeparatedLine> consumer) {
        return switch (format) {
            case Escape esc -> new BitwiseCsvEscapeSplitter(consumer, esc);
            case Quoted dbl -> new BitwiseCsvQuotedSplitter(consumer, dbl);
            case Simple smp -> new BitwiseCsvSimpleSplitter(consumer, smp);
        };
    }
}
