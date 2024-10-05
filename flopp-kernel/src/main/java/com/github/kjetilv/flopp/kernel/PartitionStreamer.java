package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.util.stream.Stream;

public interface PartitionStreamer {

    Partition partition();

    Stream<LineSegment> lines();
}
