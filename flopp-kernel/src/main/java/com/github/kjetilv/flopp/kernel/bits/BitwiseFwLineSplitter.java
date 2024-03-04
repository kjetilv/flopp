package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

final class BitwiseFwLineSplitter implements Consumer<LineSegment>, SeparatedLine {

    private final long[] start;

    private final long[] end;

    private final Consumer<SeparatedLine> lines;

    private final int length;

    private LineSegment segment;

    BitwiseFwLineSplitter(FwFormat fwFormat, Consumer<SeparatedLine> lines) {
        this.length = fwFormat.ranges().length;
        this.start = Arrays.stream(fwFormat.ranges()).mapToLong(Range::startIndex).toArray();
        this.end = Arrays.stream(fwFormat.ranges()).mapToLong(Range::endIndex).toArray();
        this.lines = Objects.requireNonNull(lines, "lines");
    }

    @Override
    public MemorySegment memorySegment() {
        return segment.memorySegment();
    }

    @Override
    public int columnCount() {
        return length;
    }

    @Override
    public long[] start() {
        return start;
    }

    @Override
    public long[] end() {
        return end;
    }

    @Override
    public void accept(LineSegment segment) {
        this.segment = Objects.requireNonNull(segment, "segment");
        lines.accept(this);
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{segment == null ? "*" : segment.asString()}]";
    }
}
