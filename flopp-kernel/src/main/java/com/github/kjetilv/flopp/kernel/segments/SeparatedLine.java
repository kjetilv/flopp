package com.github.kjetilv.flopp.kernel.segments;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface SeparatedLine {

    MemorySegment memorySegment();

    int columnCount();

    long[] start();

    long[] end();

    default Stream<String> columns(Charset charset) {
        return columns(null, charset);
    }

    default Stream<String> columns(byte[] buffer, Charset charset) {
        long[] start = start();
        long[] end = end();
        return IntStream.range(0, columnCount())
            .mapToObj(column ->
                toSegment(column, start, end).asString(buffer, charset));
    }

    @SuppressWarnings("unused")
    default Stream<LineSegment> segments() {
        long[] start = start();
        long[] end = end();
        return IntStream.range(0, columnCount())
            .mapToObj(column ->
                toSegment(column, start, end));
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
        return IntStream.range(1, columnCount())
            .map(i -> (int) (end[i] - start[i - 1]))
            .max()
            .orElse(0);
    }

    private IntFunction<String> toColumn(Charset charset) {
        long[] start = start();
        long[] end = end();
        return column ->
            toSegment(column, start, end).asString(charset);
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
