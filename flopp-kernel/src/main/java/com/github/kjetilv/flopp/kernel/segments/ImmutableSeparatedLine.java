package com.github.kjetilv.flopp.kernel.segments;

import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.lang.foreign.MemorySegment;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public record ImmutableSeparatedLine(
    MemorySegment memorySegment,
    int columnCount,
    long[] start,
    long[] end
) implements SeparatedLine {

    @Override
    public long start(int column) {
        return start[column];
    }

    @Override
    public long end(int column) {
        return end[column];
    }

    @Override
    public SeparatedLine immutableSeparatedLine() {
        return this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" +
               IntStream.range(0, columnCount)
                   .mapToObj(i -> start[i] + "-" + end[i])
                   .collect(Collectors.joining(" ")) +
               "]";
    }
}
