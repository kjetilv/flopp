package com.github.kjetilv.flopp.kernel.io;

import java.io.Closeable;
import java.util.function.Consumer;

@FunctionalInterface
public interface LinesWriter<L> extends Consumer<L>, Closeable {

    @Override
    default void close() {
    }
}
