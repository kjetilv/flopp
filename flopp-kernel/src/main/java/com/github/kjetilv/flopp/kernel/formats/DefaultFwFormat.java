package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.segments.Range;

import java.nio.charset.Charset;

record DefaultFwFormat(Range[] ranges, Charset charset) implements FwFormat {

    @Override
    public FwFormat withCharset(Charset charset) {
        return new DefaultFwFormat(ranges, charset);
    }
}
