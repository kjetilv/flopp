package com.github.kjetilv.lopp.lw;

import java.nio.charset.Charset;
import java.nio.file.Path;

public final class LinesWriters {

    public static LinesWriter simple(Path path, Charset charset) {
        return new SimpleLinesWriter(path, charset);
    }

    private LinesWriters(){

    }
}
