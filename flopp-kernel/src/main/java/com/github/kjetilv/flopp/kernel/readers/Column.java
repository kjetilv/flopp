package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.bits.MemorySegments;

import java.nio.charset.Charset;

public record Column(String name, int colunmNo, Parser parser) {

    public static Column ofBinary(String name, int columnNo) {
        return ofType(name, columnNo, toBoundedString(null));
    }

    public static Column ofString(String name, int columnNo) {
        return ofType(name, columnNo, TO_STRING);
    }

    public static Column ofString(String name, int columnNo, byte[] buffer) {
        return ofType(name, columnNo, (line, column) ->
            line.segment(column).asString(buffer));
    }

    public static Column ofBoundedString(String name, int columnNo, byte[] buffer) {
        return ofType(name, columnNo, (line, column) ->
            line.segment(column).asString(buffer));
    }

    public static Column ofType(String name, int columnNo, Parser.Obj parser) {
        return new Column(name, columnNo, parser);
    }

    public static Column ofInt(String name, int columnNo, Parser.Ing parser) {
        return new Column(name, columnNo, parser);
    }

    private static final Parser.Obj TO_STRING = (separatedLine, column) ->
        separatedLine.segment(column).asString();

    private static Parser toString(byte[] buffer) {
        return toString(buffer, null);
    }

    private static Parser.Obj toString(byte[] buffer, Charset charset) {
        return (separatedLine, column) -> separatedLine.segment(column).asString(buffer, charset);
    }

    private static Parser.Obj toBoundedString(byte[] buffer) {
        return toBoundedString(buffer, null);
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

        non-sealed interface Boo extends Parser {

            boolean parse(SeparatedLine line, int column);
        }

        non-sealed interface Ing extends Parser {

            int parse(SeparatedLine line, int column);
        }

        non-sealed interface Lon extends Parser {

            long parse(SeparatedLine line, int column);
        }

        non-sealed interface Byt extends Parser {

            byte parse(SeparatedLine line, int column);
        }

        non-sealed interface Sho extends Parser {

            short parse(SeparatedLine line, int column);
        }

        non-sealed interface Cha extends Parser {

            char parse(SeparatedLine line, int column);
        }

        non-sealed interface Dou extends Parser {

            double parse(SeparatedLine line, int column);
        }

        non-sealed interface Flo extends Parser {

            float parse(SeparatedLine line, int column);
        }
    }
}
