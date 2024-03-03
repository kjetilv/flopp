package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.util.Objects;
import java.util.function.Consumer;

final class BitwiseFwSplitter implements PartitionedSplitter {

    private final PartitionStreamer streamer;

    private final FwFormat fwFormat;

    BitwiseFwSplitter(PartitionStreamer streamer, FwFormat format) {
        this.streamer = Objects.requireNonNull(streamer, "streamer");
        this.fwFormat = Objects.requireNonNull(format, "format");
    }

    @Override
    public void process(Consumer<SeparatedLine> consumer) {
        streamer.lines().forEach(new BitwiseFwLineSplitter(fwFormat, consumer));
    }

    @Override
    public Partition partition() {
        return streamer.partition();
    }
}
