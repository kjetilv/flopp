package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.formats.CsvFormat;
import com.github.kjetilv.flopp.kernel.formats.FwFormat;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.util.function.Consumer;

final class LineSplitters {

    static LineSplitter fw(FwFormat format) {
        return fw(format, null);
    }

    static LineSplitter fw(FwFormat format, Consumer<SeparatedLine> lines) {
        return new BitwiseFwLineSplitter(format, lines);
    }

    static LineSplitter csv(CsvFormat format) {
        return csv(format, null);
    }

    static LineSplitter csv(CsvFormat format, Consumer<SeparatedLine> lines) {
        return switch (format) {
            case CsvFormat.Escape esc -> new BitwiseCsvEscapeSplitter(lines, esc);
            case CsvFormat.Quoted dbl -> new BitwiseCsvQuotedSplitter(lines, dbl);
            default -> new BitwiseCsvSimpleSplitter(lines, format);
        };
    }

    private LineSplitters() {
    }
}
