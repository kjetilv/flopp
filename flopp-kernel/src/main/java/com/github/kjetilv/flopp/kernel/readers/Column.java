package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.bits.MemorySegments;

import java.nio.charset.Charset;

public record Column<T>(String name, int colunmNo, Parser<T> parser) {

    public static Column<LineSegment> ofBinary(String name, int columnNo) {
        return ofType(name, columnNo, LineSegment::copy);
    }

    public static Column<String> ofString(String name, int columnNo) {
        return ofType(name, columnNo, TO_STRING);
    }

    public static Column<String> ofString(String name, int columnNo, byte[] buffer) {
        return ofType(name, columnNo, toString(buffer, null));
    }

    public static Column<String> ofBoundedString(String name, int columnNo, byte[] buffer) {
        return ofType(name, columnNo, toBoundedString(buffer, null));
    }

    public static <T> Column<T> ofType(String name, int columnNo, Parser<T> parser) {
        return new Column<>(name, columnNo, parser);
    }

    public Column {
        Non.negativeOrZero(colunmNo, "Columns are 1-indexed, first column is 1");
    }

    public Object parse(LineSegment lineSegment) {
        return parser.parse(lineSegment);
    }

    private static final Parser<String> TO_STRING = LineSegments::asString;

    private static Parser<String> toString(byte[] buffer) {
        return toString(buffer, null);
    }

    private static Parser<String> toString(byte[] buffer, Charset charset) {
        return lineSegment -> lineSegment.asString(buffer, charset);
    }

    private static Parser<String> toBoundedString(byte[] buffer) {
        return toBoundedString(buffer, null);
    }

    private static Parser<String> toBoundedString(byte[] buffer, Charset charset) {
        return new Parser<>() {

            @Override
            public String parse(SeparatedLine line, int column) {
                return line.column(column, charset);
            }

            @Override
            public String parse(LineSegment lineSegment) {
                return MemorySegments.fromLongsWithinBounds(
                    lineSegment.memorySegment(),
                    lineSegment.startIndex(),
                    lineSegment.endIndex(),
                    buffer,
                    charset
                );
            }
        };
    }

    @FunctionalInterface
    public interface Parser<T> {

        default T parse(SeparatedLine line, int column) {
            return parse(line.segment(column));
        }

        T parse(LineSegment lineSegment);
    }
}
