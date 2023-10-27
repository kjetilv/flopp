package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;

public interface LinesWriterFactory<T> {

    LinesWriter create(T target, Charset charset);
}
