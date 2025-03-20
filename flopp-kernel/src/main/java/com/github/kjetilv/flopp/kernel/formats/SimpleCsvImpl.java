package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.util.Non;

import java.nio.charset.Charset;
import java.util.Objects;

import static com.github.kjetilv.flopp.kernel.formats.Formats.*;

public record SimpleCsvImpl(byte separator, int columnCount, Charset charset)
    implements Format.Csv.Simple {

    public SimpleCsvImpl {
        Non.negativeOrZero(columnCount, "column count");
        Objects.requireNonNull(charset, "charset");
    }

    @Override
    public boolean fast() {
        return true;
    }

    static final Simple DEFAULT_SIMPLE =
        new SimpleCsvImpl(DEF_SEP_CHAR, DEF_COL_COUNT, DEF_CHARSET);

    @Override
    public String toString() {
        return getClass().getSimpleName() +
               "[separator:'" + (char) separator + "'" +
               " columnCount:" + columnCount +
               " charset:" + charset +
               "]";
    }
}
