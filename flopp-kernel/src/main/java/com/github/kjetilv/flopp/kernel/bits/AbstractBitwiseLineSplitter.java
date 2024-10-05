package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;
import java.util.function.Function;

abstract sealed class AbstractBitwiseLineSplitter
    implements SeparatedLine, LineSegment, Function<LineSegment, SeparatedLine>, Consumer<LineSegment>
    permits AbstractBitwiseCsvLineSplitter, BitwiseFwLineSplitter {

    private final Consumer<SeparatedLine> lines;

    private MemorySegment memorySegment;

    AbstractBitwiseLineSplitter(Consumer<SeparatedLine> lines) {
        this.lines = lines == null ? NONE : lines;
    }

    @Override
    public void accept(LineSegment lineSegment) {
        apply(lineSegment);
    }

    @Override
    public final SeparatedLine apply(LineSegment lineSegment) {
        this.memorySegment = lineSegment.memorySegment();
        process(lineSegment);
        this.lines.accept(this);
        return this;
    }

    @Override
    public final MemorySegment memorySegment() {
        return memorySegment;
    }

    @Override
    public final String toString() {
        String sub = substring();
        boolean hasSub = sub == null || sub.isBlank();
        return getClass().getSimpleName() + "[" + (hasSub ? "" : sub + " ") + memorySegment + "]";
    }

    protected abstract String substring();

    protected abstract void process(LineSegment segment);

    private static final Consumer<SeparatedLine> NONE = _ -> {
    };
}
