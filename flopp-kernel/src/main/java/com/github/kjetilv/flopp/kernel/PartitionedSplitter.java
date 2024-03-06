package com.github.kjetilv.flopp.kernel;

import java.util.function.Consumer;

public interface PartitionedSplitter {

    void forEach(Consumer<SeparatedLine> consumer);

    Partition partition();
}
