package com.github.kjetilv.flopp.kernel.qr;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public interface WriteQr<T> extends Consumer<T>, Runnable, Closeable {

    WriteQr<T> in(ExecutorService executorService);

    void awaitAllProcessed();

    @Override
    void close();
}
