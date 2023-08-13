package com.github.kjetilv.lopp;

import java.util.stream.Stream;

public interface PartitionProcessor<T> {

    T handle(Partition partitionBytes, Stream<NPLine> entries);
}
