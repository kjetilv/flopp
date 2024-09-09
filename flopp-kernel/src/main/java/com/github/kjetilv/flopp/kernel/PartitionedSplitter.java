package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.util.function.Consumer;
import java.util.stream.Stream;

public interface PartitionedSplitter {

    Partition partition();

    void forEach(Consumer<SeparatedLine> consumer);

    Stream<SeparatedLine> separatedLines();
}
