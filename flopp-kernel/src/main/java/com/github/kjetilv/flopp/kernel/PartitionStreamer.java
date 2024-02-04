package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.LineSegment;

import java.util.stream.Stream;

public interface PartitionStreamer {

    Stream<LineSegment> lines();

    Partition partition();
}
