package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.util.OptionalInt;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface SeparatedLine {

    MemorySegment memorySegment();

    int columnCount();

    long[] start();

    long[] end();

    default Stream<String> columns() {
        return columns(null);
    }

    default Stream<String> columns(Charset charset) {
        return columns(null, charset);
    }

    default Stream<String> columns(byte[] buffer, Charset charset) {
        long[] start = start();
        long[] end = end();
        return IntStream.range(0, columnCount())
            .mapToObj(column ->
                LineSegments.asString(toSegment(column, start, end), buffer, charset));
    }

    @SuppressWarnings("unused")
    default Stream<LineSegment> segments() {
        long[] start = start();
        long[] end = end();
        return IntStream.range(0, columnCount())
            .mapToObj(column ->
                toSegment(column, start, end));
    }

    default String column(int column) {
        return segment(column).asString();
    }

    default String column(int column, Charset charset) {
        return segment(column).asString(charset);
    }

    default long start(int column) {
        return start()[column];
    }

    default long end(int column) {
        return end()[column];
    }

    default LineSegment segment(int column) {
        return toSegment(column, start(), end());
    }

    default SeparatedLine immutableSeparatedLine() {
        return new ImmutableSeparatedLine(
            memorySegment(),
            columnCount(),
            copy(start()),
            copy(end())
        );
    }

    default int maxWidth() {
        long[] start = start();
        long[] end = end();
        OptionalInt max = IntStream.range(0, columnCount() - 1)
            .map(i -> (int) (end[i + 1] - start[i]))
            .max();
        return max.isPresent() ? max.getAsInt() : 0;
    }

    private IntFunction<String> toColumn(Charset charset) {
        long[] start = start();
        long[] end = end();
        return column ->
            LineSegments.asString(toSegment(column, start, end), charset);
    }

    private long[] copy(long[] ls) {
        int len = columnCount();
        long[] cp = new long[len];
        System.arraycopy(ls, 0, cp, 0, len);
        return cp;
    }

    private LineSegment toSegment(int column, long[] start, long[] end) {
        return LineSegments.of(
            memorySegment(),
            start[column],
            end[column]
        );
    }
}
