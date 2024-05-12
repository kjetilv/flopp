package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Non;

public record Column<T>(String name, int colunmNo, Parser<T> parser) {

    public static Column<LineSegment> ofBinary(String name, int columnNo) {
        return ofType(name, columnNo, LineSegment::immutable);
    }

    public static Column<String> ofString(String name, int columnNo) {
        return ofType(name, columnNo, TO_STRING);
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

    @FunctionalInterface
    public interface Parser<T> {

        T parse(LineSegment lineSegment);
    }
}
