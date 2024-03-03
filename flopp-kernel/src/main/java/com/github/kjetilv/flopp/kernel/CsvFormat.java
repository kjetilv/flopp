package com.github.kjetilv.flopp.kernel;

@SuppressWarnings("unused")
public record CsvFormat(
    char separator,
    char quote,
    char escape,
    int columnCount
) {

    public CsvFormat {
        Non.negativeOrZero(columnCount, "column count");
    }

    public CsvFormat() {
        this(DEFAULT_SEPARATOR);
    }

    public CsvFormat(char separator) {
        this(separator, DEFAULT_COLUMN_COUNT);
    }

    public CsvFormat(int columnCount) {
        this(DEFAULT_SEPARATOR, columnCount);
    }

    public CsvFormat(char separator, int columnCount) {
        this(separator, DEFAULT_QUOTE, DEFAULT_ESC, columnCount);
    }

    public CsvFormat(char separator, char quote) {
        this(separator, quote, DEFAULT_ESC);
    }

    public CsvFormat(char separator, char quote, char escape) {
        this(separator, quote, escape, DEFAULT_COLUMN_COUNT);
    }

    public CsvFormat columns(int columnCount) {
        return new CsvFormat(separator, quote, escape, columnCount);
    }

    public static final CsvFormat DEFAULT = new CsvFormat();

    public static final char DEFAULT_SEPARATOR = ',';

    public static final char DEFAULT_QUOTE = '"';

    public static final char DEFAULT_ESC = '\\';

    public static final int DEFAULT_COLUMN_COUNT = 128;
}
