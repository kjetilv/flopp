package com.github.kjetilv.lopp.qr;

import java.io.Closeable;
import java.util.stream.Stream;

public interface Vial<T> extends Closeable {

    Stream<T> contents();

    boolean done();
}
