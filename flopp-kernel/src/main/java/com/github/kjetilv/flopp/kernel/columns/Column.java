package com.github.kjetilv.flopp.kernel.columns;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.segments.MemorySegments;
import com.github.kjetilv.flopp.kernel.util.Non;

import java.nio.charset.Charset;
import java.util.Objects;
import java.util.function.Function;

@SuppressWarnings("unused")
public record Column(String name, int colunmNo, Parser parser) {

    public static Column ofSegment(String name, int col) {
        return ofType(name, col, lineSegment -> lineSegment);
    }

    public static Column ofString(String name, int col) {
        return ofType(name, col, TO_STRING);
    }

    public static Column ofString(String name, int col, byte[] buffer, Charset charset) {
        return ofType(
            name, col, segment ->
                segment.asString(buffer, charset)
        );
    }

    public static Column ofType(String name, int col, Parser.Obj parser) {
        return new Column(Objects.requireNonNull(name, "name"), col, parser);
    }

    public static Column ofInt(String name, int col, Parser.I parser) {
        return new Column(Objects.requireNonNull(name, "name"), col, parser);
    }

    public static Column ofLong(String name, int col, Parser.L parser) {
        return new Column(Objects.requireNonNull(name, "name"), col, parser);
    }

    public static Column ofBoolean(String name, int col, Parser.Bo parser) {
        return new Column(Objects.requireNonNull(name, "name"), col, parser);
    }

    public static Column ofChar(String name, int col, Parser.C parser) {
        return new Column(Objects.requireNonNull(name, "name"), col, parser);
    }

    public static Column ofByte(String name, int col, Parser.By parser) {
        return new Column(Objects.requireNonNull(name, "name"), col, parser);
    }

    public static Column ofShort(String name, int col, Parser.S parser) {
        return new Column(Objects.requireNonNull(name, "name"), col, parser);
    }

    public static Column ofFloat(String name, int col, Parser.F parser) {
        return new Column(Objects.requireNonNull(name, "name"), col, parser);
    }

    public static Column ofDouble(String name, int col, Parser.D parser) {
        return new Column(Objects.requireNonNull(name, "name"), col, parser);
    }

    public static Column ofSegment(int col) {
        return new Column(null, col, (Parser.Obj) lineSegment -> lineSegment);
    }

    public static Column ofSegment(int col, Function<LineSegment, LineSegment> fun) {
        return new Column(null, col, (Parser.Obj) fun::apply);
    }

    public static Column ofString(int col) {
        return new Column(null, col, TO_STRING);
    }

    public static Column ofString(int col, byte[] buffer, Charset charset) {
        return new Column(null, col, (Parser.Obj) segment -> segment.asString(buffer, charset));
    }

    public static Column ofType(int col, Parser.Obj parser) {
        return new Column(null, col, parser);
    }

    public static Column ofInt(int col, Parser.I parser) {
        return new Column(null, col, parser);
    }

    public static Column ofLong(int col, Parser.L parser) {
        return new Column(null, col, parser);
    }

    public static Column ofBoolean(int col, Parser.Bo parser) {
        return new Column(null, col, parser);
    }

    public static Column ofChar(int col, Parser.C parser) {
        return new Column(null, col, parser);
    }

    public static Column ofByte(int col, Parser.By parser) {
        return new Column(null, col, parser);
    }

    public static Column ofShort(int col, Parser.S parser) {
        return new Column(null, col, parser);
    }

    public static Column ofFloat(int col, Parser.F parser) {
        return new Column(null, col, parser);
    }

    public static Column ofDouble(int col, Parser.D parser) {
        return new Column(null, col, parser);
    }

    public Column {
        Non.negative(colunmNo, "columnNo");
    }

    private static final Parser.Obj TO_STRING = lineSegment -> lineSegment.asString(Charset.defaultCharset());

    private static Parser.Obj toString(byte[] buffer, Charset charset) {
        return segment -> segment.asString(buffer, charset);
    }

    private static Parser.Obj toBoundedString(byte[] buffer, Charset charset) {
        return segment ->
            MemorySegments.fromLongsWithinBounds(
                segment.memorySegment(),
                segment.startIndex(),
                segment.endIndex(),
                buffer,
                charset
            );
    }

    public sealed interface Parser {

        non-sealed interface Obj extends Parser {

            default Object parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }

            Object parse(LineSegment lineSegment);
        }

        non-sealed interface Bo extends Parser {

            default boolean parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }

            boolean parse(LineSegment segment);
        }

        non-sealed interface I extends Parser {

            default int parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }

            int parse(LineSegment segment);
        }

        non-sealed interface L extends Parser {

            default long parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }

            long parse(LineSegment segment);
        }

        non-sealed interface By extends Parser {

            default byte parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }

            byte parse(LineSegment segment);
        }

        non-sealed interface S extends Parser {

            default short parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }

            short parse(LineSegment segment);
        }

        non-sealed interface C extends Parser {

            default char parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }

            char parse(LineSegment segment);
        }

        non-sealed interface D extends Parser {

            default double parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }

            double parse(LineSegment segment);
        }

        non-sealed interface F extends Parser {

            default float parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }

            float parse(LineSegment segment);
        }
    }
}
