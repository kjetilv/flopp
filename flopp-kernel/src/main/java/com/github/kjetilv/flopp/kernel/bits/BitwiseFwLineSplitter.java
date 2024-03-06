package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

final class BitwiseFwLineSplitter extends AbstractBitwiseLineSplitter {

    private final long[] start;

    private final long[] end;

    private final int length;

    private LineSegment segment;

    BitwiseFwLineSplitter(FwFormat fwFormat, Consumer<SeparatedLine> lines) {
        this(fwFormat, lines, false);
    }

    BitwiseFwLineSplitter(FwFormat fwFormat, Consumer<SeparatedLine> lines, boolean immutable) {
        super(lines, immutable);
        this.length = fwFormat.ranges().length;
        this.start = Arrays.stream(fwFormat.ranges()).mapToLong(Range::startIndex).toArray();
        this.end = Arrays.stream(fwFormat.ranges()).mapToLong(Range::endIndex).toArray();
    }

    @Override
    protected LineSegment lineSegment() {
        return segment;
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
        emit();
    }
}
