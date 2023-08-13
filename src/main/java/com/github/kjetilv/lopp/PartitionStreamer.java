package com.github.kjetilv.lopp;

import java.util.stream.Stream;

public interface PartitionStreamer {

    Partition partition();

    Stream<NPLine> lines();
}
