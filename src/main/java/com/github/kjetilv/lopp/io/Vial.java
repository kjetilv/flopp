package com.github.kjetilv.lopp.io;

import java.io.Closeable;
import java.util.stream.Stream;

public interface Vial<T> extends Closeable {

    Stream<T> contents();

    int size();

    boolean hasResults();

    boolean done();
}
