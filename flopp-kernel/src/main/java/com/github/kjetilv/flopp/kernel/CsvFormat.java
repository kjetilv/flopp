package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;
import java.util.Objects;

@SuppressWarnings("unused")
public sealed interface CsvFormat {

    Charset charset();

    CsvFormat withCharset(Charset charset);

    char separator();

    int columnCount();

    default int maxColumnWidth() {
        return DEFAULT_MAX_COLUMN_WIDTH;
    }

    boolean fast();

    CsvFormat fast(boolean fast);

    default String[] split(String header) {
        return header.split(separatorString());
    }

    default String separatorString() {
        return Character.toString(separator());
    }

    char DEFAULT_SEPARATOR = ',';

    char DEFAULT_QUOTE = '"';

    int DEFAULT_COLUMN_COUNT = 128;

    Charset DEFAULT_CHARSET = Charset.defaultCharset();

    int DEFAULT_MAX_COLUMN_WIDTH = 8192;

    record Simple(char separator, int columnCount, Charset charset) implements CsvFormat {

        public Simple {
            Non.negativeOrZero(columnCount, "column count");
            Objects.requireNonNull(charset, "charset");
        }

        public Simple(int columnCount, char separator) {
            this(separator, columnCount, DEFAULT_CHARSET);
        }

        @Override
        public CsvFormat withCharset(Charset charset) {
            return new Simple(separator, columnCount, charset);
        }

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
                   " charset:" + charset +
                   "]";
        }
    }

    record DoubleQuoted(
        char separator,
        char quote,
        int columnCount,
        boolean fast,
        Charset charset
    ) implements CsvFormat {

        public DoubleQuoted(int columnCount, char quote, char separator) {
            this(separator, quote, columnCount, false, DEFAULT_CHARSET);
        }

        public DoubleQuoted {
            Non.negativeOrZero(columnCount, "column count");
            Objects.requireNonNull(charset, "charset");
        }

        public DoubleQuoted() {
            this(DEFAULT_SEPARATOR, DEFAULT_QUOTE, DEFAULT_COLUMN_COUNT, false, DEFAULT_CHARSET);
        }

        public DoubleQuoted(char separator) {
            this(separator, DEFAULT_QUOTE, DEFAULT_COLUMN_COUNT, false, DEFAULT_CHARSET);
        }

        public DoubleQuoted(int columnCount) {
            this(DEFAULT_SEPARATOR, DEFAULT_QUOTE, columnCount, false, DEFAULT_CHARSET);
        }

        public DoubleQuoted(char separator, char quote) {
            this(separator, quote, DEFAULT_COLUMN_COUNT, false, DEFAULT_CHARSET);
        }

        public DoubleQuoted(char separator, int columnCount) {
            this(separator, DEFAULT_QUOTE, columnCount, false, DEFAULT_CHARSET);
        }

        public DoubleQuoted columns(int columnCount) {
            return new DoubleQuoted(separator, quote, columnCount, fast, DEFAULT_CHARSET);
        }

        @Override
        public CsvFormat withCharset(Charset charset) {
            return new DoubleQuoted(separator, quote, columnCount, fast, charset);
        }

        @Override
        public CsvFormat fast(boolean fast) {
            return new DoubleQuoted(separator, quote, columnCount, fast, DEFAULT_CHARSET);
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

        public static final DoubleQuoted DEFAULT = new DoubleQuoted();
    }

    record Escaped(
        char separator,
        char escape,
        boolean fast,
        int columnCount,
        Charset charset
    ) implements CsvFormat {

        public Escaped {
            Non.negativeOrZero(columnCount, "column count");
            Objects.requireNonNull(charset, "charset");
        }

        public Escaped(boolean fast) {
            this(DEFAULT_SEPARATOR, DEFAULT_ESC, fast);
        }

        public Escaped() {
            this(DEFAULT_SEPARATOR, DEFAULT_ESC, false, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
        }

        public Escaped(char separator) {
            this(separator, DEFAULT_ESC, false, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
        }

        public Escaped(int columnCount) {
            this(DEFAULT_SEPARATOR, DEFAULT_ESC, false, columnCount, DEFAULT_CHARSET);
        }

        public Escaped(char separator, int columnCount) {
            this(separator, DEFAULT_ESC, false, columnCount, DEFAULT_CHARSET);
        }

        public Escaped(char separator, char escape) {
            this(separator, escape, false, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
        }

        public Escaped(char separator, char escape, int columnCount) {
            this(separator, escape, false, columnCount, DEFAULT_CHARSET);
        }

        public Escaped(char separator, char escape, boolean fast) {
            this(separator, escape, fast, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
        }

        public Escaped columns(int columnCount) {
            return new Escaped(separator, escape, fast, columnCount, DEFAULT_CHARSET);
        }

        @Override
        public CsvFormat withCharset(Charset charset) {
            return new Escaped(separator, escape, fast, columnCount, charset);
        }

        @Override
        public CsvFormat fast(boolean fast) {
            return new Escaped(separator, escape, fast, columnCount, DEFAULT_CHARSET);
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

        public static final Escaped DEFAULT = new Escaped();

        public static final char DEFAULT_ESC = '\\';
    }
}
