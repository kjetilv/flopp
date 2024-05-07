package com.github.kjetilv.flopp.kernel;

import java.util.List;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedSplitters {

    default Stream<PartitionedSplitter> splitters(CsvFormat format) {
        return splitters(format, false);
    }

    default Stream<PartitionedSplitter> splitters(CsvFormat format, boolean immutable) {
        return splittersList(format, immutable).stream();
    }

    default List<PartitionedSplitter> splittersList(CsvFormat format) {
        return splittersList(format, false);
    }

    List<PartitionedSplitter> splittersList(CsvFormat format, boolean immutable);

    default Stream<PartitionedSplitter> splitters(FwFormat format) {
        return splitters(format, false);
    }

    default Stream<PartitionedSplitter> splitters(FwFormat format, boolean immutable) {
        return splittersList(format, immutable).stream();
    }

    default List<PartitionedSplitter> splittersList(FwFormat format) {
        return splittersList(format, false);
    }

    List<PartitionedSplitter> splittersList(FwFormat format, boolean immutable);
}
