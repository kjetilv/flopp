package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.util.Objects;
import java.util.function.Consumer;

final class BitwiseCsvSplitter implements PartitionedSplitter {

    private final PartitionStreamer streamer;

    private final CsvFormat format;

    BitwiseCsvSplitter(PartitionStreamer streamer, CsvFormat format) {
        this.streamer = Objects.requireNonNull(streamer, "streamer");
        this.format = format == null ? CsvFormat.DEFAULT : format;
    }

    @Override
    public void process(Consumer<SeparatedLine> consumer) {
        streamer.lines().forEach(new BitwiseCsvLineSplitter(format, consumer));
    }

    @Override
    public Partition partition() {
        return streamer.partition();
    }
}
