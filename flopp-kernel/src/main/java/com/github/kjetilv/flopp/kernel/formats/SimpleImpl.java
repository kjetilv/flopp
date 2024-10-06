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

    SimpleImpl() {
        this(
            DEFAULT_COLUMN_COUNT,
            DEFAULT_SEPARATOR_CHAR
        );
    }

    SimpleImpl(int columnCount, char separator) {
        this(separator, columnCount, DEFAULT_CHARSET);
    }

    @Override
    public Simple withCharset(Charset charset) {
        return new SimpleImpl(
            separator,
            columnCount,
            charset
        );
    }

    @Override
    public Simple columns(int columnCount) {
        return new SimpleImpl(
            separator,
            columnCount,
            charset
        );
    }

    @Override
    public Simple fast(boolean fast) {
        return this;
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

    static final Simple DEFAULT_SIMPLE = new SimpleImpl();
}
