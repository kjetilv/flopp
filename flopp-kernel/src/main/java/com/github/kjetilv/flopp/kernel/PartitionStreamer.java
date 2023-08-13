package com.github.kjetilv.flopp.kernel;

import java.util.stream.Stream;

public interface PartitionStreamer {

    Stream<LineSegment> lines();

    Partition partition();
}
