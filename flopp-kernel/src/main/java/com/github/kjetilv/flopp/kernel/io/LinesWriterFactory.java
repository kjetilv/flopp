package com.github.kjetilv.flopp.kernel.io;

import java.nio.charset.Charset;

@FunctionalInterface
public interface LinesWriterFactory<T> {

    LinesWriter create(T target, Charset charset);
}
