package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.formats.CsvFormat;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

final class BitwiseCsvSplitter implements PartitionedSplitter {

    private final PartitionStreamer streamer;

    private final CsvFormat format;

    private final Partition partition;

    BitwiseCsvSplitter(PartitionStreamer streamer, CsvFormat format) {
        this.streamer = Objects.requireNonNull(streamer, "streamer");
        this.format = Objects.requireNonNull(format, "format");
        this.partition = this.streamer.partition();
    }

    @Override
    public Partition partition() {
        return partition;
    }

    @Override
    public void forEach(Consumer<SeparatedLine> consumer) {
        LineSplitter csv = LineSplitters.csv(format, consumer);
        streamer.lines().forEach(csv);
    }

    @Override
    public Stream<SeparatedLine> separatedLines() {
        LineSplitter csv = LineSplitters.csv(format);
        return streamer.lines().map(csv);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + streamer + "]";
    }
}
