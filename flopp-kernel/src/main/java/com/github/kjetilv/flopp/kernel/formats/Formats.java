package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.Range;

import java.nio.charset.Charset;

@SuppressWarnings("unused")
public final class Formats {

    public static final byte DEF_ESC_CHAR = '\\';

    public static final byte DEF_SEP_CHAR = ',';

    public static final byte DEF_QUO_CHAR = '"';

    public static final int DEF_COL_COUNT = 128;

    public static final int DEF_MAX_COL_WIDTH = 8192;

    public static final Charset DEF_CHARSET = Charset.defaultCharset();

    private Formats() {
    }

    public static final class Csv {

        public static Format.Csv.Simple simple() {
            return SimpleCsvImpl.DEFAULT_SIMPLE;
        }

        public static Format.Csv.Simple simple(char separator) {
            return simple((byte) separator);
        }

        public static Format.Csv.Simple simple(byte separator) {
            return new SimpleCsvImpl(separator, DEF_COL_COUNT, DEF_CHARSET);
        }

        public static Format.Csv.Simple simple(int columnCount, byte separator) {
            return new SimpleCsvImpl(separator, columnCount, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(int columnCount, byte quote, byte separator) {
            return new QuotedCsvImpl(separator, quote, columnCount, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted() {
            return QuotedCsvImpl.DEFAULT_QUOTED;
        }

        public static Format.Csv.Quoted quoted(char separator) {
            return quoted((byte) separator);
        }

        public static Format.Csv.Quoted quoted(byte separator) {
            return new QuotedCsvImpl(separator, DEF_QUO_CHAR, DEF_COL_COUNT, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(int columnCount) {
            return new QuotedCsvImpl(DEF_SEP_CHAR, DEF_QUO_CHAR, columnCount, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(char separator, char quote) {
            return quoted((byte) separator, (byte) quote);
        }

        public static Format.Csv.Quoted quoted(byte separator, byte quote) {
            return new QuotedCsvImpl(separator, quote, DEF_COL_COUNT, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(char separator, int columnCount) {
            return quoted((byte) separator, columnCount);
        }

        public static Format.Csv.Quoted quoted(byte separator, int columnCount) {
            return new QuotedCsvImpl(separator, DEF_QUO_CHAR, columnCount, false, DEF_CHARSET);
        }

        public static Format.Csv.Quoted quoted(
            char separator,
            char quote,
            int columnCount,
            boolean fast,
            Charset charset
        ) {
            return quoted(
                (byte) separator,
                (byte) quote,
                columnCount,
                fast,
                charset
            );
        }

        public static Format.Csv.Quoted quoted(
            byte separator,
            byte quote,
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
            return escape((byte) separator);
        }

        public static Format.Csv.Escape escape(byte separator) {
            return new EscapeCsvImpl(separator, DEF_ESC_CHAR, false, DEF_COL_COUNT, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(int columnCount) {
            return new EscapeCsvImpl(DEF_SEP_CHAR, DEF_ESC_CHAR, false, columnCount, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(char separator, int columnCount) {
            return escape((byte) separator, columnCount);
        }

        public static Format.Csv.Escape escape(byte separator, int columnCount) {
            return new EscapeCsvImpl(separator, DEF_ESC_CHAR, false, columnCount, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(char separator, char escape) {
            return escape((byte) separator, (byte) escape);
        }

        public static Format.Csv.Escape escape(byte separator, byte escape) {
            return new EscapeCsvImpl(separator, escape, false, DEF_COL_COUNT, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(char separator, char escape, int columnCount) {
            return escape((byte) separator, (byte) escape, columnCount);
        }

        public static Format.Csv.Escape escape(byte separator, byte escape, int columnCount) {
            return new EscapeCsvImpl(separator, escape, false, columnCount, DEF_CHARSET);
        }

        public static Format.Csv.Escape escape(char separator, char escape, boolean fast) {
            return escape((byte) separator, (byte) escape, fast);
        }

        public static Format.Csv.Escape escape(byte separator, byte escape, boolean fast) {
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
