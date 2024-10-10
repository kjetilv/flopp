package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings({"SameParameterValue", "unused"})
final class LineSplitters {

    static Function<LineSegment, SeparatedLine> fwTransform(Format.FwFormat format, Consumer<SeparatedLine> lines) {
        return fwLineSplitter(format, lines);
    }

    static Consumer<LineSegment> fwSink(Format.FwFormat format, Consumer<SeparatedLine> lines) {
        return fwLineSplitter(format, lines);
    }

    static Function<LineSegment, SeparatedLine> csvTransform(Format.Csv format) {
        return csvLineSplitter(format, null);
    }

    static Function<LineSegment, SeparatedLine> csvTransform(Format.Csv format, Consumer<SeparatedLine> lines) {
        return csvLineSplitter(format, lines);
    }

    static Consumer<LineSegment> csvSink(Format.Csv format, Consumer<SeparatedLine> lines) {
        return csvLineSplitter(format, lines);
    }

    private static BitwiseFwLineSplitter fwLineSplitter(Format.FwFormat format, Consumer<SeparatedLine> lines) {
        return new BitwiseFwLineSplitter(format, lines);
    }

    private static AbstractBitwiseCsvLineSplitter csvLineSplitter(
        Format.Csv format,
        Consumer<SeparatedLine> lines
    ) {
        return switch (format) {
            case Format.Csv.Escape esc -> new BitwiseCsvEscapeSplitter(lines, esc);
            case Format.Csv.Quoted dbl -> new BitwiseCsvQuotedSplitter(lines, dbl);
            default -> new BitwiseCsvSimpleSplitter(lines, format);
        };
    }

    private LineSplitters() {
    }
}
