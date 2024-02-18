package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Non;

public record LineSplit(
    char separator,
    char quote,
    char escape,
    int columnCount
) {

    public LineSplit {
        Non.negativeOrZero(columnCount, "column count");
    }

    public LineSplit() {
        this(DEFAULT_SEPARATOR);
    }

    public LineSplit(char separator) {
        this(separator, DEFAULT_COLUMN_COUNT);
    }

    public LineSplit(int columnCount) {
        this(DEFAULT_SEPARATOR, columnCount);
    }

    public LineSplit(char separator, int columnCount) {
        this(separator, DEFAULT_QUOTE, DEFAULT_ESC, columnCount);
    }

    public LineSplit(char separator, char quote) {
        this(separator, quote, DEFAULT_ESC, DEFAULT_COLUMN_COUNT);
    }

    public LineSplit columns(int columnCount) {
        return new LineSplit(separator, quote, escape, columnCount);
    }

    public static final char DEFAULT_SEPARATOR = ',';

    public static final char DEFAULT_QUOTE = '\'';

    public static final char DEFAULT_ESC = '\\';

    public static final int DEFAULT_COLUMN_COUNT = 128;
}
