package com.github.kjetilv.flopp.kernel;

import java.util.List;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedSplitters {

    default Stream<PartitionedSplitter> splitters(CsvFormat csvFormat) {
        return splitters(csvFormat, false);
    }

    default Stream<PartitionedSplitter> splitters(CsvFormat csvFormat, boolean immutable) {
        return splittersList(csvFormat, immutable).stream();
    }

    default List<PartitionedSplitter> splittersList(CsvFormat csvFormat) {
        return splittersList(csvFormat, false);
    }

    List<PartitionedSplitter> splittersList(CsvFormat csvFormat, boolean immutable);

    default Stream<PartitionedSplitter> splitters(FwFormat csvFormat) {
        return splitters(csvFormat, false);
    }

    default Stream<PartitionedSplitter> splitters(FwFormat fwFormat, boolean immutable) {
        return splittersList(fwFormat, immutable).stream();
    }

    default List<PartitionedSplitter> splittersList(FwFormat fwFormat) {
        return splittersList(fwFormat, false);
    }

    List<PartitionedSplitter> splittersList(FwFormat fwFormat, boolean immutable);
}
