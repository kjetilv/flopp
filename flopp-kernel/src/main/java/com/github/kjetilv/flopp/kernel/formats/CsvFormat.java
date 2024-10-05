package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.util.Non;

import java.nio.charset.Charset;
import java.util.Objects;

@SuppressWarnings("unused")
public sealed interface CsvFormat extends FlatFileFormat {

    static CsvFormat simple() {
        return Simple.DEFAULT;
    }

    static CsvFormat simple(char separator) {
        return new Simple.Format(DEFAULT_COLUMN_COUNT, separator);
    }

    static CsvFormat simple(int columnCount, char separator) {
        return new Simple.Format(separator, columnCount, DEFAULT_CHARSET);
    }

    static Quoted quoted(int columnCount, char quote, char separator) {
        return new Quoted.Format(separator, quote, columnCount, false, DEFAULT_CHARSET);
    }

    static Quoted quoted() {
        return Quoted.DEFAULT;
    }

    static Quoted quoted(char separator) {
        return new Quoted.Format(separator, DEFAULT_QUOTE, DEFAULT_COLUMN_COUNT, false, DEFAULT_CHARSET);
    }

    static Quoted quoted(int columnCount) {
        return new Quoted.Format(DEFAULT_SEPARATOR, DEFAULT_QUOTE, columnCount, false, DEFAULT_CHARSET);
    }

    static Quoted quoted(char separator, char quote) {
        return new Quoted.Format(separator, quote, DEFAULT_COLUMN_COUNT, false, DEFAULT_CHARSET);
    }

    static Quoted quoted(char separator, int columnCount) {
        return new Quoted.Format(separator, DEFAULT_QUOTE, columnCount, false, DEFAULT_CHARSET);
    }

    static Quoted quoted(char separator, char quote, int columnCount, boolean fast, Charset charset) {
        return new Quoted.Format(separator, quote, columnCount, false, charset);
    }

    static Escape escape(boolean fast) {
        return new Escape.Format(DEFAULT_SEPARATOR, Escape.DEFAULT_ESC, fast);
    }

    static Escape escape() {
        return Escape.DEFAULT;
    }

    static Escape escape(char separator) {
        return new Escape.Format(separator, Escape.DEFAULT_ESC, false, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
    }

    static Escape escape(int columnCount) {
        return new Escape.Format(DEFAULT_SEPARATOR, Escape.DEFAULT_ESC, false, columnCount, DEFAULT_CHARSET);
    }

    static Escape escape(char separator, int columnCount) {
        return new Escape.Format(separator, Escape.DEFAULT_ESC, false, columnCount, DEFAULT_CHARSET);
    }

    static Escape escape(char separator, char escape) {
        return new Escape.Format(separator, escape, false, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
    }

    static Escape escape(char separator, char escape, int columnCount) {
        return new Escape.Format(separator, escape, false, columnCount, DEFAULT_CHARSET);
    }

    static Escape escape(char separator, char escape, boolean fast) {
        return new Escape.Format(separator, escape, fast, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
    }

    char separator();

    int columnCount();

    CsvFormat withCharset(Charset charset);

    CsvFormat columns(int columnCount);

    CsvFormat fast(boolean fast);

    default int maxColumnWidth() {
        return DEFAULT_MAX_COLUMN_WIDTH;
    }

    boolean fast();

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

    sealed interface Simple extends CsvFormat permits Simple.Format {

        @Override
        Simple withCharset(Charset charset);

        @Override
        Simple columns(int columnCount);

        @Override
        Simple fast(boolean fast);

        Simple DEFAULT = new Format();

        record Format(char separator, int columnCount, Charset charset) implements Simple {

            public Format {
                Non.negativeOrZero(columnCount, "column count");
                Objects.requireNonNull(charset, "charset");
            }

            Format() {
                this(DEFAULT_COLUMN_COUNT, DEFAULT_SEPARATOR);
            }

            Format(int columnCount, char separator) {
                this(separator, columnCount, DEFAULT_CHARSET);
            }

            @Override
            public Simple withCharset(Charset charset) {
                return new Format(separator, columnCount, charset);
            }

            @Override
            public Simple columns(int columnCount) {
                return new Format(separator, columnCount, charset);
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
    }

    sealed interface Quoted extends CsvFormat permits Quoted.Format {

        char quote();

        @Override
        Quoted withCharset(Charset charset);

        @Override
        Quoted columns(int columnCount);

        @Override
        Quoted fast(boolean fast);

        Quoted DEFAULT = new Format();

        record Format(
            char separator,
            char quote,
            int columnCount,
            boolean fast,
            Charset charset
        ) implements Quoted {

            public Format {
                Non.negativeOrZero(columnCount, "column count");
                Objects.requireNonNull(charset, "charset");
            }

            Format() {
                this(DEFAULT_SEPARATOR, DEFAULT_QUOTE, DEFAULT_COLUMN_COUNT, false, DEFAULT_CHARSET);
            }

            public Quoted columns(int columnCount) {
                return new Format(separator, quote, columnCount, fast, DEFAULT_CHARSET);
            }

            @Override
            public Quoted withCharset(Charset charset) {
                return new Format(separator, quote, columnCount, fast, charset);
            }

            @Override
            public Quoted fast(boolean fast) {
                return new Format(separator, quote, columnCount, fast, DEFAULT_CHARSET);
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
        }
    }

    sealed interface Escape extends CsvFormat permits Escape.Format {

        char escape();

        @Override
        Escape withCharset(Charset charset);

        @Override
        Escape columns(int columnCount);

        @Override
        Escape fast(boolean fast);

        Escape DEFAULT = new Format();

        char DEFAULT_ESC = '\\';

        record Format(
            char separator,
            char escape,
            boolean fast,
            int columnCount,
            Charset charset
        ) implements Escape {

            public Format {
                Non.negativeOrZero(columnCount, "column count");
                Objects.requireNonNull(charset, "charset");
            }

            Format() {
                this(DEFAULT_SEPARATOR, DEFAULT_ESC, false, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
            }

            Format(char separator, char escape, boolean fast) {
                this(separator, escape, fast, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
            }

            public Escape columns(int columnCount) {
                return new Format(separator, escape, fast, columnCount, DEFAULT_CHARSET);
            }

            @Override
            public Escape withCharset(Charset charset) {
                return new Format(separator, escape, fast, columnCount, charset);
            }

            @Override
            public Escape fast(boolean fast) {
                return new Format(separator, escape, fast, columnCount, DEFAULT_CHARSET);
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
        }
    }
}
