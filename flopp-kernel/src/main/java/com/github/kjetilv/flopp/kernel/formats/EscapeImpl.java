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

    EscapeImpl() {
        this(
            DEFAULT_SEPARATOR_CHAR,
            DEFAULT_ESCAPE_CHAR,
            false,
            DEFAULT_COLUMN_COUNT,
            DEFAULT_CHARSET
        );
    }

    EscapeImpl(char separator, char escape, boolean fast) {
        this(
            separator,
            escape,
            fast,
            DEFAULT_COLUMN_COUNT,
            DEFAULT_CHARSET
        );
    }

    @Override
    public Escape withCharset(Charset charset) {
        return new EscapeImpl(
            separator,
            escape,
            fast,
            columnCount,
            charset
        );
    }

    public Escape columns(int columnCount) {
        return new EscapeImpl(
            separator,
            escape,
            fast,
            columnCount,
            DEFAULT_CHARSET
        );
    }

    @Override
    public Escape fast(boolean fast) {
        return new EscapeImpl(
            separator,
            escape,
            fast,
            columnCount,
            DEFAULT_CHARSET
        );
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

    static final Escape DEFAULT_ESCAPE = new EscapeImpl();
}
