package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

final class BitwiseFwSplitter implements PartitionedSplitter {

    private final PartitionStreamer streamer;

    private final Format.FwFormat format;

    private final Partition partition;

    BitwiseFwSplitter(PartitionStreamer streamer, Format.FwFormat format) {
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
        streamer.lines().forEach(LineSplitters.fwSink(format, consumer));
    }

    @Override
    public Stream<SeparatedLine> separatedLines() {
        return streamer.lines().map(LineSplitters.fwTransform(format, null));
    }
}
