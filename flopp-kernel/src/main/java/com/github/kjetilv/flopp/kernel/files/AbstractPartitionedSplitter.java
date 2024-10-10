package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

sealed abstract class AbstractPartitionedSplitter implements PartitionedSplitter
    permits BitwiseCsvSplitter, BitwiseFwSplitter {

    private final PartitionStreamer streamer;

    private final Partition partition;

    protected AbstractPartitionedSplitter(PartitionStreamer streamer) {
        this.streamer = Objects.requireNonNull(streamer, "streamer");
        this.partition = this.streamer.partition();
    }

    @Override
    public final Partition partition() {
        return partition;
    }

    @Override
    public final void forEach(Consumer<SeparatedLine> consumer) {
        streamer().lines()
            .forEach(consumer(consumer));
    }

    @Override
    public final Stream<SeparatedLine> separatedLines() {
        return streamer().lines()
            .map(transform());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + streamer() + "]";
    }

    protected abstract Consumer<LineSegment> consumer(Consumer<SeparatedLine> consumer);

    protected abstract Function<LineSegment, SeparatedLine> transform();

    protected final PartitionStreamer streamer() {
        return streamer;
    }
}
