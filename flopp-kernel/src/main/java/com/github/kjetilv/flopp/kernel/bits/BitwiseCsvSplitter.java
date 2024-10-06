package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.github.kjetilv.flopp.kernel.bits.LineSplitters.csvSink;
import static com.github.kjetilv.flopp.kernel.bits.LineSplitters.csvTransform;

final class BitwiseCsvSplitter implements PartitionedSplitter {

    private final PartitionStreamer streamer;

    private final Format.Csv format;

    private final Partition partition;

    BitwiseCsvSplitter(PartitionStreamer streamer, Format.Csv format) {
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
        streamer.lines().forEach(csvSink(format, consumer));
    }

    @Override
    public Stream<SeparatedLine> separatedLines() {
        return streamer.lines().map(csvTransform(format));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + streamer + "]";
    }
}
