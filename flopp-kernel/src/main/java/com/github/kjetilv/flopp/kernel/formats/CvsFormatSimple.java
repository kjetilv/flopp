package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.util.Non;

import java.nio.charset.Charset;
import java.util.Objects;

record CvsFormatSimple(char separator, int columnCount, Charset charset) implements CsvFormat.Simple {

    CvsFormatSimple {
        Non.negativeOrZero(columnCount, "column count");
        Objects.requireNonNull(charset, "charset");
    }

    CvsFormatSimple() {
        this(DEFAULT_COLUMN_COUNT, DEFAULT_SEPARATOR);
    }

    CvsFormatSimple(int columnCount, char separator) {
        this(separator, columnCount, DEFAULT_CHARSET);
    }

    @Override
    public Simple withCharset(Charset charset) {
        return new CvsFormatSimple(separator, columnCount, charset);
    }

    @Override
    public Simple columns(int columnCount) {
        return new CvsFormatSimple(separator, columnCount, charset);
    }

    @Override
    public boolean fast() {
        return true;
    }

    @Override
    public Simple fast(boolean fast) {
        return this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() +
               "[separator:" + separator +
               " columnCount:" + columnCount +
               " charset:" + charset +
               "]";
    }
}
