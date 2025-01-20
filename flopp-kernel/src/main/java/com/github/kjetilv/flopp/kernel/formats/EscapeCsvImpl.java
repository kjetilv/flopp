package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.util.Non;

import java.nio.charset.Charset;
import java.util.Objects;

import static com.github.kjetilv.flopp.kernel.formats.Formats.*;

public record EscapeCsvImpl(
    char separator,
    char escape,
    boolean fast,
    int columnCount,
    Charset charset
) implements Format.Csv.Escape {

    public EscapeCsvImpl {
        Non.negativeOrZero(columnCount, "column count");
        Objects.requireNonNull(charset, "charset");
    }

    static final Escape DEFAULT_ESCAPE =
        new EscapeCsvImpl(DEF_SEP_CHAR, DEF_ESC_CHAR, false, DEF_COL_COUNT, DEF_CHARSET);

    @SuppressWarnings("NullableProblems")
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
