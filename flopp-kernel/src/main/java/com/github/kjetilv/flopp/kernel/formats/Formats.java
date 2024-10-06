package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.segments.Range;

import java.nio.charset.Charset;

@SuppressWarnings("unused")
public final class Formats {

    private Formats() {
    }

    public static final char DEFAULT_ESCAPE_CHAR = '\\';

    public static final char DEFAULT_SEPARATOR_CHAR = ',';

    public static final char DEFAULT_QUOTE_CHAR = '"';

    public static final int DEFAULT_COLUMN_COUNT = 128;

    public static final int DEFAULT_MAX_COLUMN_WIDTH = 8192;

    public static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    public static final class Csv {

        public static Format.Csv simple() {
            return SimpleImpl.DEFAULT_SIMPLE;
        }

        public static Format.Csv simple(char separator) {
            return new SimpleImpl(
                DEFAULT_COLUMN_COUNT,
                separator
            );
        }

        public static Format.Csv simple(int columnCount, char separator) {
            return new SimpleImpl(
                separator,
                columnCount,
                DEFAULT_CHARSET
            );
        }

        public static Format.Csv.Quoted quoted(int columnCount, char quote, char separator) {
            return new QuotedImpl(
                separator,
                quote,
                columnCount,
                false,
                DEFAULT_CHARSET
            );
        }

        public static Format.Csv.Quoted quoted() {
            return QuotedImpl.DEFAULT_QUOTED;
        }

        public static Format.Csv.Quoted quoted(char separator) {
            return new QuotedImpl(
                separator,
                DEFAULT_QUOTE_CHAR,
                DEFAULT_COLUMN_COUNT,
                false,
                DEFAULT_CHARSET
            );
        }

        public static Format.Csv.Quoted quoted(int columnCount) {
            return new QuotedImpl(
                DEFAULT_SEPARATOR_CHAR,
                DEFAULT_QUOTE_CHAR,
                columnCount,
                false,
                DEFAULT_CHARSET
            );
        }

        public static Format.Csv.Quoted quoted(char separator, char quote) {
            return new QuotedImpl(
                separator,
                quote,
                DEFAULT_COLUMN_COUNT,
                false,
                DEFAULT_CHARSET
            );
        }

        public static Format.Csv.Quoted quoted(char separator, int columnCount) {
            return new QuotedImpl(
                separator,
                DEFAULT_QUOTE_CHAR,
                columnCount,
                false,
                DEFAULT_CHARSET
            );
        }

        public static Format.Csv.Quoted quoted(
            char separator,
            char quote,
            int columnCount,
            boolean fast,
            Charset charset
        ) {
            return new QuotedImpl(
                separator,
                quote,
                columnCount,
                false,
                charset
            );
        }

        public static Format.Csv.Escape escape(boolean fast) {
            return new EscapeImpl(
                DEFAULT_SEPARATOR_CHAR,
                DEFAULT_ESCAPE_CHAR,
                fast
            );
        }

        public static Format.Csv.Escape escape() {
            return EscapeImpl.DEFAULT_ESCAPE;
        }

        public static Format.Csv.Escape escape(char separator) {
            return new EscapeImpl(
                separator,
                DEFAULT_ESCAPE_CHAR,
                false,
                DEFAULT_COLUMN_COUNT,
                DEFAULT_CHARSET
            );
        }

        public static Format.Csv.Escape escape(int columnCount) {
            return new EscapeImpl(
                DEFAULT_SEPARATOR_CHAR,
                DEFAULT_ESCAPE_CHAR,
                false,
                columnCount,
                DEFAULT_CHARSET
            );
        }

        public static Format.Csv.Escape escape(char separator, int columnCount) {
            return new EscapeImpl(
                separator,
                DEFAULT_ESCAPE_CHAR,
                false,
                columnCount,
                DEFAULT_CHARSET
            );
        }

        public static Format.Csv.Escape escape(char separator, char escape) {
            return new EscapeImpl(
                separator,
                escape,
                false,
                DEFAULT_COLUMN_COUNT,
                DEFAULT_CHARSET
            );
        }

        public static Format.Csv.Escape escape(char separator, char escape, int columnCount) {
            return new EscapeImpl(
                separator,
                escape,
                false,
                columnCount,
                DEFAULT_CHARSET
            );
        }

        public static Format.Csv.Escape escape(char separator, char escape, boolean fast) {
            return new EscapeImpl(
                separator,
                escape,
                fast,
                DEFAULT_COLUMN_COUNT,
                DEFAULT_CHARSET
            );
        }

        private Csv() {
        }
    }

    public static final class Fw {

        public static Format.FwFormat simple(Range... ranges) {
            return new FwFormatImpl(ranges, null);
        }

        public static Format.FwFormat simple(Charset charset, Range... ranges) {
            return new FwFormatImpl(ranges, charset);
        }

        private Fw() {
        }
    }
}
