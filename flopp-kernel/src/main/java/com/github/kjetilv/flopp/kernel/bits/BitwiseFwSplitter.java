package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.util.Objects;
import java.util.function.Consumer;

final class BitwiseFwSplitter implements PartitionedSplitter {

    private final PartitionStreamer streamer;

    private final FwFormat fwFormat;

    private final boolean immutable;

    BitwiseFwSplitter(PartitionStreamer streamer, FwFormat format, boolean immutable) {
        this.streamer = Objects.requireNonNull(streamer, "streamer");
        this.fwFormat = Objects.requireNonNull(format, "format");
        this.immutable = immutable;
    }

    @Override
    public void process(Consumer<SeparatedLine> consumer) {
        streamer.lines().forEach(new BitwiseFwLineSplitter(fwFormat, consumer, immutable));
    }

    @Override
    public Partition partition() {
        return streamer.partition();
    }
}
