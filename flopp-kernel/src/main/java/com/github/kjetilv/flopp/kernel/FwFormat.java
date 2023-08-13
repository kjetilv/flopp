package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;

public record FwFormat(Range[] ranges, Charset charset) {

    public FwFormat(Range... ranges) {
        this(ranges, null);
    }

    public FwFormat withCharset(Charset charset) {
        return new FwFormat(ranges, charset);
    }
}
