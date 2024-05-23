package com.github.kjetilv.flopp.kernel;

import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedSplitters {

    Stream<PartitionedSplitter> splitters(FwFormat format);

    Stream<PartitionedSplitter> splitters(CsvFormat format);
}
