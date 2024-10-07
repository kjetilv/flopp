package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.Range;

import java.nio.charset.Charset;

public sealed interface Format {

    Charset charset();

    @SuppressWarnings("unused")
    sealed interface Csv extends Format {

        char separator();

        int columnCount();

        default int maxColumnWidth() {
            return Formats.DEF_MAX_COL_WIDTH;
        }

        boolean fast();

        default String[] split(String header) {
            return header.split(separatorString());
        }

        default String separatorString() {
            return Character.toString(separator());
        }

        sealed interface Quoted extends Csv permits QuotedImpl {

            char quote();
        }

        sealed interface Escape extends Csv permits EscapeImpl {

            char escape();
        }

        sealed interface Simple extends Csv permits SimpleImpl {
        }
    }

    sealed interface FwFormat extends Format permits FwFormatImpl {

        Range[] ranges();
    }
}
