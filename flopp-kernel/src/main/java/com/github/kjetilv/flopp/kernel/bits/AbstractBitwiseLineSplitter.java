package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSplitter;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;

@SuppressWarnings("PackageVisibleField")
abstract sealed class AbstractBitwiseLineSplitter
    implements LineSplitter, SeparatedLine, LineSegment
    permits AbstractBitwiseCsvLineSplitter, BitwiseFwLineSplitter {

    MemorySegment segment;

    private final Consumer<SeparatedLine> lines;

    AbstractBitwiseLineSplitter(Consumer<SeparatedLine> lines) {
        this.lines = lines == null ? NONE : lines;
    }

    @Override
    public final SeparatedLine apply(LineSegment lineSegment) {
        init(lineSegment);
        separate(lineSegment);
        markEnd();
        this.lines.accept(this);
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

    abstract void init(LineSegment lineSegment);

    String substring() {
        return null;
    }

    void separate(LineSegment segment) {
    }

    void markEnd() {
    }

    private static final Consumer<SeparatedLine> NONE = _ -> {
    };
}
