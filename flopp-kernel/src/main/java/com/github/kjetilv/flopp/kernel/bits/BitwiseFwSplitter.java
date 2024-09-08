package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;
import com.github.kjetilv.flopp.kernel.formats.FwFormat;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

final class BitwiseFwSplitter implements PartitionedSplitter {

    private final PartitionStreamer streamer;

    private final FwFormat format;

    BitwiseFwSplitter(PartitionStreamer streamer, FwFormat format) {
        this.streamer = Objects.requireNonNull(streamer, "streamer");
        this.format = Objects.requireNonNull(format, "format");
    }

    @Override
    public void forEach(Consumer<SeparatedLine> consumer) {
        streamer.lines().forEach(LineSplitters.fw(format, consumer));
    }

    @Override
    public Stream<SeparatedLine> separatedLines() {
        return streamer.lines().map(LineSplitters.fw(format, null));
    }
}
