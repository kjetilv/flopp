package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.util.Non;

import java.nio.charset.Charset;
import java.util.Objects;

record CvsFormatEscape(
    char separator,
    char escape,
    boolean fast,
    int columnCount,
    Charset charset
) implements CsvFormat.Escape {

    CvsFormatEscape {
        Non.negativeOrZero(columnCount, "column count");
        Objects.requireNonNull(charset, "charset");
    }

    CvsFormatEscape() {
        this(DEFAULT_SEPARATOR, DEFAULT_ESC, false, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
    }

    CvsFormatEscape(char separator, char escape, boolean fast) {
        this(separator, escape, fast, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
    }

    public Escape columns(int columnCount) {
        return new CvsFormatEscape(separator, escape, fast, columnCount, DEFAULT_CHARSET);
    }

    @Override
    public Escape withCharset(Charset charset) {
        return new CvsFormatEscape(separator, escape, fast, columnCount, charset);
    }

    @Override
    public Escape fast(boolean fast) {
        return new CvsFormatEscape(separator, escape, fast, columnCount, DEFAULT_CHARSET);
    }

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
