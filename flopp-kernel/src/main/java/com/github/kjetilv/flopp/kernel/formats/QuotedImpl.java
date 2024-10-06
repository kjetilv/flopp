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

    QuotedImpl() {
        this(
            DEFAULT_SEPARATOR_CHAR,
            DEFAULT_QUOTE_CHAR,
            DEFAULT_COLUMN_COUNT,
            false,
            DEFAULT_CHARSET
        );
    }

    @Override
    public Quoted withCharset(Charset charset) {
        return new QuotedImpl(
            separator,
            quote,
            columnCount,
            fast,
            charset
        );
    }

    public Quoted columns(int columnCount) {
        return new QuotedImpl(
            separator,
            quote,
            columnCount,
            fast,
            DEFAULT_CHARSET
        );
    }

    @Override
    public Quoted fast(boolean fast) {
        return new QuotedImpl(
            separator,
            quote,
            columnCount,
            fast,
            DEFAULT_CHARSET
        );
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

    static final Quoted DEFAULT_QUOTED = new QuotedImpl();
}
