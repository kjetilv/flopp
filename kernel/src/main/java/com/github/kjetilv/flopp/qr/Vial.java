package com.github.kjetilv.flopp.qr;

import java.io.Closeable;
import java.util.stream.Stream;

public interface Vial<T> extends Closeable {

    Stream<T> contents();

    boolean done();
}
