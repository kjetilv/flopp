package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;
import java.util.function.Function;

abstract sealed class AbstractBitwiseLineSplitter
    implements Function<LineSegment, SeparatedLine>, Consumer<LineSegment>, SeparatedLine
    permits AbstractBitwiseCsvLineSplitter, BitwiseFwLineSplitter {

    private final Consumer<SeparatedLine> lines;

    @SuppressWarnings("PackageVisibleField")
    MemorySegment segment;

    AbstractBitwiseLineSplitter(Consumer<SeparatedLine> lines) {
        this.lines = lines == null ? NONE : lines;
    }

    @Override
    public final void accept(LineSegment segment) {
        apply(segment);
    }

    @Override
    public final SeparatedLine apply(LineSegment lineSegment) {
        segment = lineSegment.memorySegment();
        separate(lineSegment);
        lines.accept(this);
        return this;
    }

    @Override
    public final MemorySegment memorySegment() {
        return segment;
    }

    @Override
    public final String toString() {
        String sub = substring();
        boolean hasSub = sub == null || sub.isBlank();
        return getClass().getSimpleName() + "[" + (hasSub ? "" : sub + " ") + segment + "]";
    }

    String substring() {
        return null;
    }

    protected abstract void separate(LineSegment segment);

    private static final Consumer<SeparatedLine> NONE = _ -> {
    };
}
