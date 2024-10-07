package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.util.Non;

import java.nio.charset.Charset;
import java.util.Objects;

import static com.github.kjetilv.flopp.kernel.formats.Formats.*;

record QuotedImpl(
    char separator,
    char quote,
    int columnCount,
    boolean fast,
    Charset charset
) implements Format.Csv.Quoted {

    QuotedImpl {
        Non.negativeOrZero(columnCount, "column count");
        Objects.requireNonNull(charset, "charset");
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() +
               "[separator:" + separator +
               " quote:" + quote +
               " columnCount:" + columnCount +
               " fast:" + fast +
               " charset:" + charset +
               "]";
    }

    static final Quoted DEFAULT_QUOTED =
        new QuotedImpl(DEF_SEP_CHAR, DEF_QUO_CHAR, DEF_COL_COUNT, false, DEF_CHARSET);
}
