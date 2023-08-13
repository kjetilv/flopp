package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;

@FunctionalInterface
public interface LinesWriterFactory<T> {

    LinesWriter create(T target, Charset charset);
}
