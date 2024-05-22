package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.bits.MemorySegments;

import java.nio.charset.Charset;

public record Column(String name, int colunmNo, Parser parser) {

    public static Column ofString(String name, int columnNo) {
        return ofType(name, columnNo, TO_STRING);
    }

    public static Column ofString(String name, int columnNo, byte[] buffer) {
        return ofType(name, columnNo, (line, column) ->
            line.segment(column).asString(buffer));
    }

    public static Column ofType(String name, int columnNo, Parser.Obj parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofInt(String name, int columnNo, Parser.I parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofLong(String name, int columnNo, Parser.L parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofBoolean(String name, int columnNo, Parser.Bo parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofChar(String name, int columnNo, Parser.C parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofByte(String name, int columnNo, Parser.By parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofShort(String name, int columnNo, Parser.S parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofFloat(String name, int columnNo, Parser.F parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofDouble(String name, int columnNo, Parser.D parser) {
        return new Column(name, columnNo, parser);
    }

    private static final Parser.Obj TO_STRING = (separatedLine, column) ->
        separatedLine.segment(column).asString();

    private static Parser.Obj toString(byte[] buffer, Charset charset) {
        return (separatedLine, column) -> separatedLine.segment(column).asString(buffer, charset);
    }

    private static Parser.Obj toBoundedString(byte[] buffer, Charset charset) {
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

    public sealed interface Parser {

        non-sealed interface Obj extends Parser {

            Object parse(SeparatedLine line, int column);
        }

        non-sealed interface Bo extends Parser {

            boolean parse(SeparatedLine line, int column);
        }

        non-sealed interface I extends Parser {

            int parse(SeparatedLine line, int column);
        }

        non-sealed interface L extends Parser {

            long parse(SeparatedLine line, int column);
        }

        non-sealed interface By extends Parser {

            byte parse(SeparatedLine line, int column);
        }

        non-sealed interface S extends Parser {

            short parse(SeparatedLine line, int column);
        }

        non-sealed interface C extends Parser {

            char parse(SeparatedLine line, int column);
        }

        non-sealed interface D extends Parser {

            double parse(SeparatedLine line, int column);
        }

        non-sealed interface F extends Parser {

            float parse(SeparatedLine line, int column);
        }
    }
}
