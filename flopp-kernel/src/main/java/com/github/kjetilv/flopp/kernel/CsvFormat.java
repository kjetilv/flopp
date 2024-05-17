package com.github.kjetilv.flopp.kernel;

@SuppressWarnings("unused")
public sealed interface CsvFormat {

    char separator();

    int columnCount();

    boolean fast();

    CsvFormat fast(boolean fast);

    char DEFAULT_SEPARATOR = ',';

    char DEFAULT_QUOTE = '"';

    int DEFAULT_COLUMN_COUNT = 128;

    default String[] split(String header) {
        return header.split(separatorString());
    }

    default String separatorString() {
        return Character.toString(separator());
    }

    record Simple(char separator, int columnCount) implements CsvFormat {

        @Override
        public boolean fast() {
            return true;
        }

        @Override
        public CsvFormat fast(boolean fast) {
            return this;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() +
                   "[separator:" + separator +
                   " columnCount:" + columnCount +
                   "]";
        }
    }

    record DoubleQuoted(
        char separator,
        char quote,
        int columnCount,
        boolean fast
    ) implements CsvFormat {

        public DoubleQuoted(int columnCount, char quote, char separator) {
            this(separator, quote, columnCount, false);
        }

        public DoubleQuoted {
            Non.negativeOrZero(columnCount, "column count");
        }

        public DoubleQuoted() {
            this(DEFAULT_SEPARATOR, DEFAULT_QUOTE, DEFAULT_COLUMN_COUNT, false);
        }

        public DoubleQuoted(char separator) {
            this(separator, DEFAULT_QUOTE, DEFAULT_COLUMN_COUNT, false);
        }

        public DoubleQuoted(int columnCount) {
            this(DEFAULT_SEPARATOR, DEFAULT_QUOTE, columnCount, false);
        }

        public DoubleQuoted(char separator, char quote) {
            this(separator, quote, DEFAULT_COLUMN_COUNT, false);
        }

        public DoubleQuoted(char separator, int columnCount) {
            this(separator, DEFAULT_QUOTE, columnCount, false);
        }

        public DoubleQuoted columns(int columnCount) {
            return new DoubleQuoted(separator, quote, columnCount, fast);
        }

        @Override
        public CsvFormat fast(boolean fast) {
            return new DoubleQuoted(separator, quote, columnCount, fast);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() +
                   "[separator:" + separator +
                   " quote:" + quote +
                   " columnCount:" + columnCount +
                   " fast:" + fast +
                   "]";
        }

        public static final DoubleQuoted DEFAULT = new DoubleQuoted();
    }

    record Escaped(
        char separator,
        char escape,
        boolean fast,
        int columnCount
    ) implements CsvFormat {

        public static final Escaped DEFAULT = new Escaped();

        public Escaped {
            Non.negativeOrZero(columnCount, "column count");
        }

        public Escaped(boolean fast) {
            this(DEFAULT_SEPARATOR, DEFAULT_ESC, fast);
        }

        public Escaped() {
            this(DEFAULT_SEPARATOR, DEFAULT_ESC, false, DEFAULT_COLUMN_COUNT);
        }

        public Escaped(char separator) {
            this(separator, DEFAULT_ESC, false, DEFAULT_COLUMN_COUNT);
        }

        public Escaped(int columnCount) {
            this(DEFAULT_SEPARATOR, DEFAULT_ESC, false, columnCount);
        }

        public Escaped(char separator, int columnCount) {
            this(separator, DEFAULT_ESC, false, columnCount);
        }

        public Escaped(char separator, char escape) {
            this(separator, escape, false, DEFAULT_COLUMN_COUNT);
        }

        public Escaped(char separator, char escape, int columnCount) {
            this(separator, escape, false, columnCount);
        }

        public Escaped(char separator, char escape, boolean fast) {
            this(separator, escape, fast, DEFAULT_COLUMN_COUNT);
        }

        public Escaped columns(int columnCount) {
            return new Escaped(separator, escape, fast, columnCount);
        }

        @Override
        public CsvFormat fast(boolean fast) {
            return new Escaped(separator, escape, fast, columnCount);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() +
                   "[separator:" + separator +
                   " escape:" + escape +
                   " fast:" + fast +
                   " columnCount:" + columnCount +
                   "]";
        }

        public static final char DEFAULT_ESC = '\\';
    }
}
