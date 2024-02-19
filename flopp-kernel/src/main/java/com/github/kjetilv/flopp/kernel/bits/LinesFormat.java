package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Non;

public record LinesFormat(
    char separator,
    char quote,
    char escape,
    int columnCount
) {

    public LinesFormat {
        Non.negativeOrZero(columnCount, "column count");
    }

    public LinesFormat() {
        this(DEFAULT_SEPARATOR);
    }

    public LinesFormat(char separator) {
        this(separator, DEFAULT_COLUMN_COUNT);
    }

    public LinesFormat(int columnCount) {
        this(DEFAULT_SEPARATOR, columnCount);
    }

    public LinesFormat(char separator, int columnCount) {
        this(separator, DEFAULT_QUOTE, DEFAULT_ESC, columnCount);
    }

    public LinesFormat(char separator, char quote) {
        this(separator, quote, DEFAULT_ESC);
    }

    public LinesFormat(char separator, char quote, char escape) {
        this(separator, quote, escape, DEFAULT_COLUMN_COUNT);
    }

    public LinesFormat columns(int columnCount) {
        return new LinesFormat(separator, quote, escape, columnCount);
    }

    public static final char DEFAULT_SEPARATOR = ',';

    public static final char DEFAULT_QUOTE = '"';

    public static final char DEFAULT_ESC = '\\';

    public static final int DEFAULT_COLUMN_COUNT = 128;
}
