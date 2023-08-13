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

    record Quoted(
        char separator,
        char quote,
        int columnCount,
        boolean fast,
        Charset charset
    ) implements CsvFormat {

        public Quoted(int columnCount, char quote, char separator) {
            this(separator, quote, columnCount, false, DEFAULT_CHARSET);
        }

        public Quoted {
            Non.negativeOrZero(columnCount, "column count");
            Objects.requireNonNull(charset, "charset");
        }

        public Quoted() {
            this(DEFAULT_SEPARATOR, DEFAULT_QUOTE, DEFAULT_COLUMN_COUNT, false, DEFAULT_CHARSET);
        }

        public Quoted(char separator) {
            this(separator, DEFAULT_QUOTE, DEFAULT_COLUMN_COUNT, false, DEFAULT_CHARSET);
        }

        public Quoted(int columnCount) {
            this(DEFAULT_SEPARATOR, DEFAULT_QUOTE, columnCount, false, DEFAULT_CHARSET);
        }

        public Quoted(char separator, char quote) {
            this(separator, quote, DEFAULT_COLUMN_COUNT, false, DEFAULT_CHARSET);
        }

        public Quoted(char separator, int columnCount) {
            this(separator, DEFAULT_QUOTE, columnCount, false, DEFAULT_CHARSET);
        }

        public Quoted columns(int columnCount) {
            return new Quoted(separator, quote, columnCount, fast, DEFAULT_CHARSET);
        }

        @Override
        public CsvFormat withCharset(Charset charset) {
            return new Quoted(separator, quote, columnCount, fast, charset);
        }

        @Override
        public CsvFormat fast(boolean fast) {
            return new Quoted(separator, quote, columnCount, fast, DEFAULT_CHARSET);
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

        public static final Quoted DEFAULT = new Quoted();
    }

    record Escape(
        char separator,
        char escape,
        boolean fast,
        int columnCount,
        Charset charset
    ) implements CsvFormat {

        public Escape {
            Non.negativeOrZero(columnCount, "column count");
            Objects.requireNonNull(charset, "charset");
        }

        public Escape(boolean fast) {
            this(DEFAULT_SEPARATOR, DEFAULT_ESC, fast);
        }

        public Escape() {
            this(DEFAULT_SEPARATOR, DEFAULT_ESC, false, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
        }

        public Escape(char separator) {
            this(separator, DEFAULT_ESC, false, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
        }

        public Escape(int columnCount) {
            this(DEFAULT_SEPARATOR, DEFAULT_ESC, false, columnCount, DEFAULT_CHARSET);
        }

        public Escape(char separator, int columnCount) {
            this(separator, DEFAULT_ESC, false, columnCount, DEFAULT_CHARSET);
        }

        public Escape(char separator, char escape) {
            this(separator, escape, false, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
        }

        public Escape(char separator, char escape, int columnCount) {
            this(separator, escape, false, columnCount, DEFAULT_CHARSET);
        }

        public Escape(char separator, char escape, boolean fast) {
            this(separator, escape, fast, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
        }

        public Escape columns(int columnCount) {
            return new Escape(separator, escape, fast, columnCount, DEFAULT_CHARSET);
        }

        @Override
        public CsvFormat withCharset(Charset charset) {
            return new Escape(separator, escape, fast, columnCount, charset);
        }

        @Override
        public CsvFormat fast(boolean fast) {
            return new Escape(separator, escape, fast, columnCount, DEFAULT_CHARSET);
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

        public static final Escape DEFAULT = new Escape();

        public static final char DEFAULT_ESC = '\\';
    }
}
