package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.util.function.Consumer;

public interface LinesWriter extends Consumer<String>, Closeable {

    @Override
    void close();
}
