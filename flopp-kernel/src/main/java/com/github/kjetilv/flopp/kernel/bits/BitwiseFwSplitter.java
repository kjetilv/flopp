package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

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
    public Partition partition() {
        return streamer.partition();
    }

    @Override
    public void forEach(Consumer<SeparatedLine> consumer) {
        streamer.lines().forEach(new BitwiseFwLineSplitter(format, consumer));
    }

    @Override
    public Stream<SeparatedLine> separatedLine() {
        return streamer.lines().map(new BitwiseFwLineSplitter(format, null));
    }
}
