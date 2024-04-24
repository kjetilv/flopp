package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface SeparatedLine {

    MemorySegment memorySegment();

    int columnCount();

    long[] start();

    long[] end();

    default Stream<String> columns() {
        return IntStream.range(0, columnCount()).mapToObj(toColumn());
    }

    @SuppressWarnings("unused")
    default Stream<LineSegment> segments() {
        return IntStream.range(0, columnCount()).mapToObj(toSegment());
    }

    default String column(int column) {
        return segment(column).asString();
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

    default SeparatedLine immutable() {
        return new ImmutableSeparatedLine(
            memorySegment(),
            columnCount(),
            copy(start()),
            copy(end())
        );
    }

    private IntFunction<LineSegment> toSegment() {
        long[] start = start();
        long[] end = end();
        return column ->
            toSegment(column, start, end);
    }

    private IntFunction<String> toColumn() {
        long[] start = start();
        long[] end = end();
        return column ->
            toSegment(column, start, end).asString();
    }

    private long[] copy(long[] ls) {
        int len = columnCount();
        long[] cp = new long[len];
        System.arraycopy(ls, 0, cp, 0, len);
        return cp;
    }

    private LineSegment toSegment(int column, long[] start, long[] end) {
        return LineSegments.of(memorySegment(), start[column], end[column]);
    }
}
