package com.github.kjetilv.flopp.kernel;

import java.util.stream.Stream;

public interface PartitionStreamer {

    Partition partition();

    Stream<LineSegment> lines();
}
