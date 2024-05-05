package com.github.kjetilv.flopp.kernel;

import java.util.function.Consumer;
import java.util.stream.Stream;

public interface PartitionedSplitter {

    Partition partition();

    void forEach(Consumer<SeparatedLine> consumer);

    Stream<SeparatedLine> separatedLine();
}
