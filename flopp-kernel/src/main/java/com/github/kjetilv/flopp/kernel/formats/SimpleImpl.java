package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.util.Non;

import java.nio.charset.Charset;
import java.util.Objects;

import static com.github.kjetilv.flopp.kernel.formats.Formats.*;

record SimpleImpl(char separator, int columnCount, Charset charset)
    implements Format.Csv.Simple {

    SimpleImpl {
        Non.negativeOrZero(columnCount, "column count");
        Objects.requireNonNull(charset, "charset");
    }

    @Override
    public boolean fast() {
        return true;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() +
               "[separator:" + separator +
               " columnCount:" + columnCount +
               " charset:" + charset +
               "]";
    }

    static final Simple DEFAULT_SIMPLE =
        new SimpleImpl(DEF_SEP_CHAR, DEF_COL_COUNT, DEF_CHARSET);
}