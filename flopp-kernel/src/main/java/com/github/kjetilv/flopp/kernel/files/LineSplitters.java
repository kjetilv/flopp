package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.kjetilv.flopp.kernel.util.Todo.TODO;

@SuppressWarnings({"SameParameterValue", "unused"})
final class LineSplitters {

    private LineSplitters() {
    }

    static final class Bitwise {

        static Function<LineSegment, SeparatedLine> fwTransform(Format.FwFormat format, Consumer<SeparatedLine> lines) {
            return bitwiseFwLineSplitter(format, lines);
        }

        static Consumer<LineSegment> fwSink(Format.FwFormat format, Consumer<SeparatedLine> lines) {
            return bitwiseFwLineSplitter(format, lines);
        }

        static Function<LineSegment, SeparatedLine> csvTransform(Format.Csv format) {
            return bitwiseCsvLineSplitter(format, null);
        }

        static Function<LineSegment, SeparatedLine> csvTransform(Format.Csv format, Consumer<SeparatedLine> lines) {
            return bitwiseCsvLineSplitter(format, lines);
        }

        static Consumer<LineSegment> csvSink(Format.Csv format, Consumer<SeparatedLine> lines) {
            return bitwiseCsvLineSplitter(format, lines);
        }

        private Bitwise() {
        }

        private static BitwiseFwLineSplitter bitwiseFwLineSplitter(
            Format.FwFormat format,
            Consumer<SeparatedLine> lines
        ) {
            return new BitwiseFwLineSplitter(format, lines);
        }

        private static AbstractBitwiseCsvLineSplitter bitwiseCsvLineSplitter(
            Format.Csv format,
            Consumer<SeparatedLine> lines
        ) {
            return switch (format) {
                case Format.Csv.Escape esc -> new BitwiseCsvEscapeSplitter(lines, esc);
                case Format.Csv.Quoted dbl -> new BitwiseCsvQuotedSplitter(lines, dbl);
                default -> new BitwiseCsvSimpleSplitter(lines, format);
            };
        }
    }

    static final class Vector {

        public static Consumer<LineSegment> fwSink(Format.FwFormat format, Consumer<SeparatedLine> consumer) {
            return null;
        }

        public static Function<LineSegment, SeparatedLine> fwTransform(
            Format.FwFormat format,
            Consumer<SeparatedLine> consumer
        ) {
            return TODO();
        }

        public static Consumer<LineSegment> csvSink(Format.Csv format, Consumer<SeparatedLine> consumer) {
            return switch (format) {
                case Format.Csv.Escape esc -> TODO();
                case Format.Csv.Quoted dbl -> TODO();
                default -> new VectorCsvSimpleLineSplitter(format, consumer);
            };

        }

        public static Function<LineSegment, SeparatedLine> csvTransform(Format.Csv format) {
            return csvTransform(format, null);
        }

        public static Function<LineSegment, SeparatedLine> csvTransform(
            Format.Csv format,
            Consumer<SeparatedLine> lines
        ) {
            return null;
        }
    }
}
