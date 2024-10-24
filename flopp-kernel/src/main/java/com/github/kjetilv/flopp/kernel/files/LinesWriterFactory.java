package com.github.kjetilv.flopp.kernel.files;

@FunctionalInterface
public interface LinesWriterFactory<T, L> {

    LinesWriter<L> create(T target);
}
