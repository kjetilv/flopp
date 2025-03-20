package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.formats.*;

import java.nio.charset.Charset;

public sealed interface Format {

    Charset charset();

    @SuppressWarnings("unused")
    sealed interface Csv extends Format {

        default int maxColumnWidth() {
            return Formats.DEF_MAX_COL_WIDTH;
        }

        default String[] split(String header) {
            return header.split(separatorString());
        }

        default String separatorString() {
            return Character.toString(separator());
        }

        byte separator();

        int columnCount();

        boolean fast();

        sealed interface Quoted extends Csv permits QuotedCsvImpl {

            byte quote();
        }

        sealed interface Escape extends Csv permits EscapeCsvImpl {

            byte escape();
        }

        sealed interface Simple extends Csv permits SimpleCsvImpl {
        }
    }

    sealed interface FwFormat extends Format permits FwFormatImpl {

        Range[] ranges();
    }
}
