package com.github.kjetilv.lopp.io;

import java.io.Closeable;
import java.util.Collection;

public interface Vein<T> extends Closeable {
    void inject(T t);

    void inject(Collection<T> ts);

    Vial<T> tap();

    void drain();

    @Override
    void close();
}
