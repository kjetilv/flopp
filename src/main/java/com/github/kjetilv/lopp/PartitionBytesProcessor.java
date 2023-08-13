package com.github.kjetilv.lopp;

import java.util.stream.Stream;

public interface PartitionBytesProcessor<T> {

    T handle(PartitionBytes partitionBytes, Stream<NPLine> entries);
}
