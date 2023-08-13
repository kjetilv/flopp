package com.github.kjetilv.lopp.lw;

import java.io.Closeable;
import java.util.function.Consumer;

public interface LinesWriter extends Consumer<String>, Closeable {

    @Override
    void close();
}
