package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings("PackageVisibleField")
abstract sealed class AbstractBitwiseLineSplitter
    implements Function<LineSegment, SeparatedLine>, Consumer<LineSegment>, SeparatedLine
    permits AbstractBitwiseCsvLineSplitter, BitwiseFwLineSplitter {

    private final Consumer<SeparatedLine> lines;

    private final boolean immutable;

    LineSegment segment;

    long underlyingSize;

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
        this.segment = Objects.requireNonNull(segment, "segment");
        underlyingSize = this.segment.underlyingSize();
        return doApply(this.segment);
    }

    @Override
    public final MemorySegment memorySegment() {
        return segment.memorySegment();
    }

    protected abstract SeparatedLine doApply(LineSegment segment);

    final SeparatedLine emit(SeparatedLine separatedLine) {
        lines.accept(separatedLine);
        return separatedLine;
    }

    final SeparatedLine sl() {
        return immutable ? immutableSeparatedLine() : this;
    }

    public final long underlyingSize() {
        return underlyingSize;
    }

    String substring() {
        return null;
    }

    @Override
    public final String toString() {
        String sub = substring();
        boolean hasSub = sub == null || sub.isBlank();
        return getClass().getSimpleName() + "[" +
               (hasSub ? "" : sub + " ") + "`" + (segment == null ? "*" : segment.asString()) + "`" +
               "]";
    }
}
