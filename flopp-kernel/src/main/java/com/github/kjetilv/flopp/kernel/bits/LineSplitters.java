package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.formats.CsvFormat;
import com.github.kjetilv.flopp.kernel.formats.FwFormat;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings({"SameParameterValue", "unused"})
final class LineSplitters {

    static Function<LineSegment, SeparatedLine> fwTransform(FwFormat format, Consumer<SeparatedLine> lines) {
        return fwLineSplitter(format, lines);
    }

    static Consumer<LineSegment> fwSink(FwFormat format, Consumer<SeparatedLine> lines) {
        return fwLineSplitter(format, lines);
    }

    static Function<LineSegment, SeparatedLine> csvTransform(CsvFormat format) {
        return csvLineSplitter(format, null);
    }

    static Function<LineSegment, SeparatedLine> csvTransform(CsvFormat format, Consumer<SeparatedLine> lines) {
        return csvLineSplitter(format, lines);
    }

    static Consumer<LineSegment> csvSink(CsvFormat format, Consumer<SeparatedLine> lines) {
        return csvLineSplitter(format, lines);
    }

    private static BitwiseFwLineSplitter fwLineSplitter(FwFormat format, Consumer<SeparatedLine> lines) {
        return new BitwiseFwLineSplitter(format, lines);
    }

    private static AbstractBitwiseCsvLineSplitter csvLineSplitter(
        CsvFormat format,
        Consumer<SeparatedLine> lines
    ) {
        return switch (format) {
            case CsvFormat.Escape esc -> new BitwiseCsvEscapeSplitter(lines, esc);
            case CsvFormat.Quoted dbl -> new BitwiseCsvQuotedSplitter(lines, dbl);
            default -> new BitwiseCsvSimpleSplitter(lines, format);
        };
    }

    private LineSplitters() {
    }
}
