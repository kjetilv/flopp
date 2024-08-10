package com.github.kjetilv.flopp.kernel.formats;

import java.nio.charset.Charset;

@SuppressWarnings("unused")
public sealed interface CsvFormat {

    static CsvFormat simple() {
        return Simple.DEFAULT;
    }

    static CsvFormat simple(char separator) {
        return new CvsFormatSimple(DEFAULT_COLUMN_COUNT, separator);
    }

    static CsvFormat simple(int columnCount, char separator) {
        return new CvsFormatSimple(separator, columnCount, DEFAULT_CHARSET);
    }

    static Quoted quoted(int columnCount, char quote, char separator) {
        return new CvsFormatQuoted(separator, quote, columnCount, false, DEFAULT_CHARSET);
    }

    static Quoted quoted() {
        return Quoted.DEFAULT;
    }

    static Quoted quoted(char separator) {
        return new CvsFormatQuoted(separator, DEFAULT_QUOTE, DEFAULT_COLUMN_COUNT, false, DEFAULT_CHARSET);
    }

    static Quoted quoted(int columnCount) {
        return new CvsFormatQuoted(DEFAULT_SEPARATOR, DEFAULT_QUOTE, columnCount, false, DEFAULT_CHARSET);
    }

    static Quoted quoted(char separator, char quote) {
        return new CvsFormatQuoted(separator, quote, DEFAULT_COLUMN_COUNT, false, DEFAULT_CHARSET);
    }

    static Quoted quoted(char separator, int columnCount) {
        return new CvsFormatQuoted(separator, DEFAULT_QUOTE, columnCount, false, DEFAULT_CHARSET);
    }

    static Quoted quoted(char separator, char quote, int columnCount, boolean fast, Charset charset) {
        return new CvsFormatQuoted(separator, quote, columnCount, false, charset);
    }

    static Escape escape(boolean fast) {
        return new CvsFormatEscape(DEFAULT_SEPARATOR, Escape.DEFAULT_ESC, fast);
    }

    static Escape escape() {
        return Escape.DEFAULT;
    }

    static Escape escape(char separator) {
        return new CvsFormatEscape(separator, Escape.DEFAULT_ESC, false, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
    }

    static Escape escape(int columnCount) {
        return new CvsFormatEscape(DEFAULT_SEPARATOR, Escape.DEFAULT_ESC, false, columnCount, DEFAULT_CHARSET);
    }

    static Escape escape(char separator, int columnCount) {
        return new CvsFormatEscape(separator, Escape.DEFAULT_ESC, false, columnCount, DEFAULT_CHARSET);
    }

    static Escape escape(char separator, char escape) {
        return new CvsFormatEscape(separator, escape, false, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
    }

    static Escape escape(char separator, char escape, int columnCount) {
        return new CvsFormatEscape(separator, escape, false, columnCount, DEFAULT_CHARSET);
    }

    static Escape escape(char separator, char escape, boolean fast) {
        return new CvsFormatEscape(separator, escape, fast, DEFAULT_COLUMN_COUNT, DEFAULT_CHARSET);
    }

    Charset charset();

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

    sealed interface Simple extends CsvFormat permits CvsFormatSimple {

        @Override
        Simple withCharset(Charset charset);

        @Override
        Simple columns(int columnCount);

        @Override
        Simple fast(boolean fast);

        Simple DEFAULT = new CvsFormatSimple();
    }

    sealed interface Quoted extends CsvFormat permits CvsFormatQuoted {

        char quote();

        @Override
        Quoted withCharset(Charset charset);

        @Override
        Quoted columns(int columnCount);

        @Override
        Quoted fast(boolean fast);

        Quoted DEFAULT = new CvsFormatQuoted();
    }

    sealed interface Escape extends CsvFormat permits CvsFormatEscape {

        char escape();

        @Override
        Escape withCharset(Charset charset);

        @Override
        Escape columns(int columnCount);

        @Override
        Escape fast(boolean fast);

        Escape DEFAULT = new CvsFormatEscape();

        char DEFAULT_ESC = '\\';
    }
}
