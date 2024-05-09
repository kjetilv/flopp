package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings("PackageVisibleField")
abstract sealed class AbstractBitwiseLineSplitter
    implements Function<LineSegment, SeparatedLine>, Consumer<LineSegment>, SeparatedLine
    permits AbstractBitwiseCsvLineSplitter, BitwiseFwLineSplitter {

    LineSegment segment;

    long underlyingSize;

    private final Consumer<SeparatedLine> lines;

    private final boolean immutable;

    MemorySegment memorySegment;

    AbstractBitwiseLineSplitter(Consumer<SeparatedLine> lines, boolean immutable) {
        this.lines = lines == null ? _ -> {} : lines;
        this.immutable = immutable;
    }

    @Override
    public final void accept(LineSegment segment) {
        apply(segment);
    }

    @Override
    public final SeparatedLine apply(LineSegment segment) {
        this.segment = segment;
        memorySegment = segment.memorySegment();
        underlyingSize = memorySegment.byteSize();
        separate();
        SeparatedLine separatedLine = immutable ? immutableSeparatedLine() : this;
        lines.accept(separatedLine);
        return separatedLine;
    }

    @Override
    public final MemorySegment memorySegment() {
        return segment.memorySegment();
    }

    public final long underlyingSize() {
        return underlyingSize;
    }

    @Override
    public final String toString() {
        String sub = substring();
        boolean hasSub = sub == null || sub.isBlank();
        return getClass().getSimpleName() + "[" +
               (hasSub ? "" : sub + " ") + "`" + (segment == null ? "*" : segment.asString()) + "`" +
               "]";
    }

    protected void separate() {
    }

    String substring() {
        return null;
    }
}
