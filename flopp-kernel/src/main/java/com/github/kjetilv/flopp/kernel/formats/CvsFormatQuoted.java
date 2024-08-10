package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.util.Non;

import java.nio.charset.Charset;
import java.util.Objects;

record CvsFormatQuoted(
    char separator,
    char quote,
    int columnCount,
    boolean fast,
    Charset charset
) implements CsvFormat.Quoted {

    CvsFormatQuoted {
        Non.negativeOrZero(columnCount, "column count");
        Objects.requireNonNull(charset, "charset");
    }

    CvsFormatQuoted() {
        this(DEFAULT_SEPARATOR, DEFAULT_QUOTE, DEFAULT_COLUMN_COUNT, false, DEFAULT_CHARSET);
    }

    public Quoted columns(int columnCount) {
        return new CvsFormatQuoted(separator, quote, columnCount, fast, DEFAULT_CHARSET);
    }

    @Override
    public Quoted withCharset(Charset charset) {
        return new CvsFormatQuoted(separator, quote, columnCount, fast, charset);
    }

    @Override
    public Quoted fast(boolean fast) {
        return new CvsFormatQuoted(separator, quote, columnCount, fast, DEFAULT_CHARSET);
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
}
