package com.github.kjetilv.lopp;

import java.util.stream.Stream;

public interface PartitionConsumer {

    void handle(PartitionBytes partitionBytes, Stream<NPLine> entries);
}
