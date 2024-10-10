package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.segments.Range;

import java.nio.charset.Charset;

@SuppressWarnings("unused")
public final class Formats {

    private Formats() {
    }

    static final char DEF_ESC_CHAR = '\\';

    static final char DEF_SEP_CHAR = ',';

    static final char DEF_QUO_CHAR = '"';

    static final int DEF_COL_COUNT = 128;

    static final int DEF_MAX_COL_WIDTH = 8192;

    static final Charset DEF_CHARSET = Charset.defaultCharset();

    public static final class Csv {

        public static Format.Csv.Simple simple() {
            return SimpleImpl.DEFAULT_SIMPLE;
        }

        public static Format.Csv.Simple simple(char separator) {
            return new SimpleImpl(separator, DEF_COL_COUNT, DEF_CHARSET);
        }

        public static Format.Csv.Simple simple(int columnCount, char separator) {
            return new SimpleImpl(separator, columnCount, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(int columnCount, char quote, char separator) {
            return new QuotedImpl(separator, quote, columnCount, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted() {
            return QuotedImpl.DEFAULT_QUOTED;
        }

        public static Format.Csv.Quoted quoted(char separator) {
            return new QuotedImpl(separator, DEF_QUO_CHAR, DEF_COL_COUNT, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(int columnCount) {
            return new QuotedImpl(DEF_SEP_CHAR, DEF_QUO_CHAR, columnCount, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(char separator, char quote) {
            return new QuotedImpl(separator, quote, DEF_COL_COUNT, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(char separator, int columnCount) {
            return new QuotedImpl(separator, DEF_QUO_CHAR, columnCount, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(
            char separator,
            char quote,
            int columnCount,
            boolean fast,
            Charset charset
        ) {
            return new QuotedImpl(separator, quote, columnCount, false, charset);
        }

        public static Format.Csv.Escape escape(boolean fast) {
            return new EscapeImpl(DEF_SEP_CHAR, DEF_ESC_CHAR, fast, DEF_COL_COUNT, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape() {
            return EscapeImpl.DEFAULT_ESCAPE;
        }

        public static Format.Csv.Escape escape(char separator) {
            return new EscapeImpl(separator, DEF_ESC_CHAR, false, DEF_COL_COUNT, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(int columnCount) {
            return new EscapeImpl(DEF_SEP_CHAR, DEF_ESC_CHAR, false, columnCount, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(char separator, int columnCount) {
            return new EscapeImpl(separator, DEF_ESC_CHAR, false, columnCount, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(char separator, char escape) {
            return new EscapeImpl(separator, escape, false, DEF_COL_COUNT, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(char separator, char escape, int columnCount) {
            return new EscapeImpl(separator, escape, false, columnCount, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(char separator, char escape, boolean fast) {
            return new EscapeImpl(separator, escape, fast, DEF_COL_COUNT, DEF_CHARSET);
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
