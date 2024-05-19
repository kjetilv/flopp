package com.github.kjetilv.flopp.kernel;

import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedSplitters {

    default Stream<PartitionedSplitter> splitters(CsvFormat format) {
        return splitters(format, false);
    }

    default Stream<PartitionedSplitter> splitters(FwFormat format) {
        return splitters(format, false);
    }

    Stream<PartitionedSplitter> splitters(FwFormat format, boolean immutable);

    Stream<PartitionedSplitter> splitters(CsvFormat format, boolean immutable);
}
