package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.PartitionedSplitter;

import java.util.function.Consumer;

@FunctionalInterface
public interface Reader {

    void read(PartitionedSplitter splitter, Consumer<Columns> values);

    default Reader copy() {
        throw new UnsupportedOperationException();
    }

    interface Columns {

        Object get(String name);
    }
}
