package com.github.kjetilv.flopp.kernel;

import java.util.function.Consumer;

public interface PartitionedSplitter {

    Partition partition();

    void forEach(Consumer<SeparatedLine> consumer);
}
