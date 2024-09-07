package com.github.kjetilv.flopp.kernel.io;

import java.nio.charset.Charset;

@FunctionalInterface
public interface LinesWriterFactory<T, L> {

    LinesWriter<L> create(T target, Charset charset);
}
