package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.util.Non;

import java.nio.charset.Charset;
import java.util.Objects;

import static com.github.kjetilv.flopp.kernel.formats.Formats.*;

record QuotedCsvImpl(
    char separator,
    char quote,
    int columnCount,
    boolean fast,
    Charset charset
) implements Format.Csv.Quoted {

    QuotedCsvImpl {
        Non.negativeOrZero(columnCount, "column count");
        Objects.requireNonNull(charset, "charset");
    }

    static final Quoted DEFAULT_QUOTED =
        new QuotedCsvImpl(DEF_SEP_CHAR, DEF_QUO_CHAR, DEF_COL_COUNT, false, DEF_CHARSET);

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
