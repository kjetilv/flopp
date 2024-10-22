package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.util.Non;

import java.nio.charset.Charset;
import java.util.Objects;

import static com.github.kjetilv.flopp.kernel.formats.Formats.*;

record EscapeImpl(
    char separator,
    char escape,
    boolean fast,
    int columnCount,
    Charset charset
) implements Format.Csv.Escape {

    EscapeImpl {
        Non.negativeOrZero(columnCount, "column count");
        Objects.requireNonNull(charset, "charset");
    }

    static final Escape DEFAULT_ESCAPE =
        new EscapeImpl(DEF_SEP_CHAR, DEF_ESC_CHAR, false, DEF_COL_COUNT, DEF_CHARSET);

    @Override
    public String toString() {
        return getClass().getSimpleName() +
               "[separator:" + separator +
               " escape:" + escape +
               " fast:" + fast +
               " columnCount:" + columnCount +
               " charset:" + charset +
               "]";
    }
}
