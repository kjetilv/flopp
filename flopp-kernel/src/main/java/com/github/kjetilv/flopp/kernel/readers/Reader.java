package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.PartitionedSplitter;

import java.util.function.Consumer;

@FunctionalInterface
public interface Reader {

    void read(PartitionedSplitter splitter, Consumer<Columns> values);

    interface Columns {

        Object get(String name);
    }
}
