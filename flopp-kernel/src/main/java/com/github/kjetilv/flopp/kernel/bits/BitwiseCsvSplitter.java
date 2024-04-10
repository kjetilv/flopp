package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

final class BitwiseCsvSplitter implements PartitionedSplitter {

    private final PartitionStreamer streamer;

    private final CsvFormat format;

    private final boolean immutable;

    BitwiseCsvSplitter(
        PartitionStreamer streamer,
        CsvFormat format,
        boolean immutable
    ) {
        this.streamer = Objects.requireNonNull(streamer, "streamer");
        this.format = format == null ? CsvFormat.DEFAULT : format;
        this.immutable = immutable;
    }

    @Override
    public Partition partition() {
        return streamer.partition();
    }

    @Override
    public void forEach(Consumer<SeparatedLine> consumer) {
        Consumer<LineSegment> splitter =
            new BitwiseCsvLineSplitter(format, consumer, immutable);
        lines().forEach(splitter);
    }

    @Override
    public Stream<LineSegment> lines() {
        return streamer.lines();
    }
}
