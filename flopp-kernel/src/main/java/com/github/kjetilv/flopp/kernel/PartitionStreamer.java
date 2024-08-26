package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.util.stream.Stream;

public interface PartitionStreamer {

    Stream<LineSegment> lines();

    Partition partition();
}
