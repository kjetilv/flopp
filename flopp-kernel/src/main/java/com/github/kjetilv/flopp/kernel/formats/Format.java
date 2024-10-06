package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.segments.Range;

import java.nio.charset.Charset;

public sealed interface Format<F extends Format<F>> {

    Charset charset();

    F withCharset(Charset charset);

    @SuppressWarnings("unused")
    sealed interface Csv extends Format<Csv> {

        char separator();

        int columnCount();

        Csv withCharset(Charset charset);

        Csv columns(int columnCount);

        Csv fast(boolean fast);

        default int maxColumnWidth() {
            return Formats.DEFAULT_MAX_COLUMN_WIDTH;
        }

        boolean fast();

        default String[] split(String header) {
            return header.split(separatorString());
        }

        default String separatorString() {
            return Character.toString(separator());
        }

        sealed interface Quoted
            extends Csv
            permits QuotedImpl {

            char quote();

            @Override
            Quoted withCharset(Charset charset);

            @Override
            Quoted columns(int columnCount);

            @Override
            Quoted fast(boolean fast);

        }

        sealed interface Escape
            extends Csv
            permits EscapeImpl {

            char escape();

            @Override
            Escape withCharset(Charset charset);

            @Override
            Escape columns(int columnCount);

            @Override
            Escape fast(boolean fast);

        }

        sealed interface Simple
            extends Csv
            permits SimpleImpl {

            @Override
            Simple withCharset(Charset charset);

            @Override
            Simple columns(int columnCount);

            @Override
            Simple fast(boolean fast);
        }
    }

    sealed interface FwFormat
        extends Format<FwFormat>
        permits FwFormatImpl {

        Range[] ranges();
    }
}
