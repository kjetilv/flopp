package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.Objects;
import java.util.function.Consumer;

abstract class AbstractBitwiseLineSplitter implements Consumer<LineSegment>, SeparatedLine {

    private final Consumer<SeparatedLine> lines;

    private final boolean immutable;

    AbstractBitwiseLineSplitter(Consumer<SeparatedLine> lines, boolean immutable) {
        this.lines = Objects.requireNonNull(lines, "lines");
        this.immutable = immutable;
    }

    protected final void emit() {
        lines.accept(immutable ? immutableSeparatedLine() : this);
    }

    protected abstract LineSegment lineSegment();

    @Override
    public final String toString() {
        String sub = substring();
        return getClass().getSimpleName() + "[" +
               (sub == null ? "" : sub + " ") + (lineSegment() == null ? "*" : lineSegment().asString()) +
               "]";
    }

    protected String substring() {
        return null;
    }
}
