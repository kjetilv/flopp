package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.segments.Range;

import java.nio.charset.Charset;

public sealed interface FwFormat extends FlatFileFormat {

    static FwFormat fw(Range... ranges) {
        return fw(null, ranges);
    }

    static FwFormat fw(Charset charset, Range... ranges) {
        return new DefaultFwFormat(ranges, charset);
    }

    FwFormat withCharset(Charset charset);

    Range[] ranges();

    record DefaultFwFormat(Range[] ranges, Charset charset) implements FwFormat {

        @Override
        public FwFormat withCharset(Charset charset) {
            return new DefaultFwFormat(ranges, charset);
        }
    }
}
