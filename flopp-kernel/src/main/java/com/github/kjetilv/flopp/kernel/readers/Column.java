package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Non;

public record Column(String name, int colunmNo, Parser parser) {

    public Column {
        Non.negativeOrZero(colunmNo, "Columns are 1-indexed, first column is 1");
    }

    public Column(String name, int colunmNo) {
        this(name, colunmNo, TO_STRING);
    }

    public Object parse(LineSegment lineSegment) {
        return parser.parse(lineSegment);
    }

    public static final Parser TO_STRING = LineSegments::asString;

    @FunctionalInterface
    public interface Parser {

        Object parse(LineSegment lineSegment);
    }
}
