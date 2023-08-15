package com.github.kjetilv.lopp;

import java.nio.charset.Charset;

public interface LinesWriterFactory<T> {

    LinesWriter create(T target, Charset charset);
}
