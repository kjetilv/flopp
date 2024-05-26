package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.bits.MemorySegments;

import java.nio.charset.Charset;

@SuppressWarnings("unused")
public record Column(String name, int colunmNo, Parse parse) {

    public static Column ofSegment(String name, int col) {
        return ofType(name, col, lineSegment -> lineSegment);
    }

    public static Column ofString(String name, int col) {
        return ofType(name, col, TO_STRING);
    }

    public static Column ofString(String name, int col, byte[] buffer, Charset charset) {
        return ofType(name, col, segment ->
            segment.asString(buffer, charset));
    }

    public static Column ofType(String name, int col, Parse.Obj parser) {
        return new Column(name, col, parser);
    }

    public static Column ofInt(String name, int col, Parse.I parser) {
        return new Column(name, col, parser);
    }

    public static Column ofLong(String name, int col, Parse.L parser) {
        return new Column(name, col, parser);
    }

    public static Column ofBoolean(String name, int col, Parse.Bo parser) {
        return new Column(name, col, parser);
    }

    public static Column ofChar(String name, int col, Parse.C parser) {
        return new Column(name, col, parser);
    }

    public static Column ofByte(String name, int col, Parse.By parser) {
        return new Column(name, col, parser);
    }

    public static Column ofShort(String name, int col, Parse.S parser) {
        return new Column(name, col, parser);
    }

    public static Column ofFloat(String name, int col, Parse.F parser) {
        return new Column(name, col, parser);
    }

    public static Column ofDouble(String name, int col, Parse.D parser) {
        return new Column(name, col, parser);
    }

    private static final Parse.Obj TO_STRING = lineSegment -> lineSegment.asString(Charset.defaultCharset());

    private static Parse.Obj toString(byte[] buffer, Charset charset) {
        return segment -> segment.asString(buffer, charset);
    }

    private static Parse.Obj toBoundedString(byte[] buffer, Charset charset) {
        return segment ->
            MemorySegments.fromLongsWithinBounds(
                segment.memorySegment(),
                segment.startIndex(),
                segment.endIndex(),
                buffer,
                charset
            );
    }

    public sealed interface Parse {

        non-sealed interface Obj extends Parse {

            Object parse(LineSegment lineSegment);

            default Object parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }
        }

        non-sealed interface Bo extends Parse {

            boolean parse(LineSegment segment);

            default boolean parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }
        }

        non-sealed interface I extends Parse {

            int parse(LineSegment segment);

            default int parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }
        }

        non-sealed interface L extends Parse {

            long parse(LineSegment segment);

            default long parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }
        }

        non-sealed interface By extends Parse {

            byte parse(LineSegment segment);

            default byte parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }
        }

        non-sealed interface S extends Parse {

            short parse(LineSegment segment);

            default short parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }
        }

        non-sealed interface C extends Parse {

            char parse(LineSegment segment);

            default char parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }
        }

        non-sealed interface D extends Parse {

            double parse(LineSegment segment);

            default double parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }
        }

        non-sealed interface F extends Parse {

            float parse(LineSegment segment);

            default float parse(SeparatedLine line, int column) {
                return parse(line.segment(column));
            }
        }
    }
}
