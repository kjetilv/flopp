package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.bits.MemorySegments;

import java.nio.charset.Charset;

public record Column(String name, int colunmNo, Parse parse) {

    public static Column ofString(String name, int columnNo) {
        return ofType(name, columnNo, TO_STRING);
    }

    public static Column ofString(String name, int columnNo, byte[] buffer) {
        return ofType(name, columnNo, (line, column) ->
            line.segment(column).asString(buffer));
    }

    public static Column ofType(String name, int columnNo, Parse.Obj parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofInt(String name, int columnNo, Parse.I parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofLong(String name, int columnNo, Parse.L parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofBoolean(String name, int columnNo, Parse.Bo parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofChar(String name, int columnNo, Parse.C parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofByte(String name, int columnNo, Parse.By parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofShort(String name, int columnNo, Parse.S parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofFloat(String name, int columnNo, Parse.F parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofDouble(String name, int columnNo, Parse.D parser) {
        return new Column(name, columnNo, parser);
    }

    private static final Parse.Obj TO_STRING = (separatedLine, column) ->
        separatedLine.segment(column).asString();

    private static Parse.Obj toString(byte[] buffer, Charset charset) {
        return (separatedLine, column) -> separatedLine.segment(column).asString(buffer, charset);
    }

    private static Parse.Obj toBoundedString(byte[] buffer, Charset charset) {
        return (line, column) -> {
            LineSegment lineSegment = line.segment(column);
            return MemorySegments.fromLongsWithinBounds(
                lineSegment.memorySegment(),
                lineSegment.startIndex(),
                lineSegment.endIndex(),
                buffer,
                charset
            );
        };
    }

    public sealed interface Parse {

        non-sealed interface Obj extends Parse {

            Object parse(SeparatedLine line, int column);
        }

        non-sealed interface Bo extends Parse {

            boolean parse(SeparatedLine line, int column);
        }

        non-sealed interface I extends Parse {

            int parse(SeparatedLine line, int column);
        }

        non-sealed interface L extends Parse {

            long parse(SeparatedLine line, int column);
        }

        non-sealed interface By extends Parse {

            byte parse(SeparatedLine line, int column);
        }

        non-sealed interface S extends Parse {

            short parse(SeparatedLine line, int column);
        }

        non-sealed interface C extends Parse {

            char parse(SeparatedLine line, int column);
        }

        non-sealed interface D extends Parse {

            double parse(SeparatedLine line, int column);
        }

        non-sealed interface F extends Parse {

            float parse(SeparatedLine line, int column);
        }
    }
}
