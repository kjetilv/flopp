package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.Range;

import java.nio.charset.Charset;

@SuppressWarnings("unused")
public final class Formats {

    private Formats() {
    }

    public static final char DEF_ESC_CHAR = '\\';

    public static final char DEF_SEP_CHAR = ',';

    public static final char DEF_QUO_CHAR = '"';

    public static final int DEF_COL_COUNT = 128;

    public static final int DEF_MAX_COL_WIDTH = 8192;

    public static final Charset DEF_CHARSET = Charset.defaultCharset();

    public static final class Csv {

        public static Format.Csv.Simple simple() {
            return SimpleCsvImpl.DEFAULT_SIMPLE;
        }

        public static Format.Csv.Simple simple(char separator) {
            return new SimpleCsvImpl(separator, DEF_COL_COUNT, DEF_CHARSET);
        }

        public static Format.Csv.Simple simple(int columnCount, char separator) {
            return new SimpleCsvImpl(separator, columnCount, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(int columnCount, char quote, char separator) {
            return new QuotedCsvImpl(separator, quote, columnCount, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted() {
            return QuotedCsvImpl.DEFAULT_QUOTED;
        }

        public static Format.Csv.Quoted quoted(char separator) {
            return new QuotedCsvImpl(separator, DEF_QUO_CHAR, DEF_COL_COUNT, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(int columnCount) {
            return new QuotedCsvImpl(DEF_SEP_CHAR, DEF_QUO_CHAR, columnCount, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(char separator, char quote) {
            return new QuotedCsvImpl(separator, quote, DEF_COL_COUNT, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(char separator, int columnCount) {
            return new QuotedCsvImpl(separator, DEF_QUO_CHAR, columnCount, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(
            char separator,
            char quote,
            int columnCount,
            boolean fast,
            Charset charset
        ) {
            return new QuotedCsvImpl(separator, quote, columnCount, false, charset);
        }

        public static Format.Csv.Escape escape(boolean fast) {
            return new EscapeCsvImpl(DEF_SEP_CHAR, DEF_ESC_CHAR, fast, DEF_COL_COUNT, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape() {
            return EscapeCsvImpl.DEFAULT_ESCAPE;
        }

        public static Format.Csv.Escape escape(char separator) {
            return new EscapeCsvImpl(separator, DEF_ESC_CHAR, false, DEF_COL_COUNT, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(int columnCount) {
            return new EscapeCsvImpl(DEF_SEP_CHAR, DEF_ESC_CHAR, false, columnCount, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(char separator, int columnCount) {
            return new EscapeCsvImpl(separator, DEF_ESC_CHAR, false, columnCount, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(char separator, char escape) {
            return new EscapeCsvImpl(separator, escape, false, DEF_COL_COUNT, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(char separator, char escape, int columnCount) {
            return new EscapeCsvImpl(separator, escape, false, columnCount, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(char separator, char escape, boolean fast) {
            return new EscapeCsvImpl(separator, escape, fast, DEF_COL_COUNT, DEF_CHARSET);
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
