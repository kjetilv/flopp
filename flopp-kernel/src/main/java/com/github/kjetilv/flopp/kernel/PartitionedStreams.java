package com.github.kjetilv.flopp.kernel;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedStreams {

    default Stream<? extends PartitionStreamer> streamers() {
        return streamers(false);
    }

    default Stream<? extends PartitionStreamer> streamers(boolean immutable) {
        return streamersList(immutable).stream();
    }

    default Stream<LongSupplier> lineCounters() {
        return lineCountersList().stream();
    }

    default List<? extends PartitionStreamer> streamersList() {
        return streamersList(false);
    }

    default Stream<Consumer<Consumer<SeparatedLine>>> lineSplitters(
        LinesFormat linesFormat
    ) {
        return lineSplittersList(linesFormat).stream();
    }

    List<? extends PartitionStreamer> streamersList(boolean copying);

    List<LongSupplier> lineCountersList();

    List<Consumer<Consumer<SeparatedLine>>> lineSplittersList(
        LinesFormat linesFormat
    );
}
