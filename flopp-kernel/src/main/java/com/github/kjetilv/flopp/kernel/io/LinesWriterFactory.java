package com.github.kjetilv.flopp.kernel.io;

@FunctionalInterface
public interface LinesWriterFactory<T, L> {

    LinesWriter<L> create(T target);
}
