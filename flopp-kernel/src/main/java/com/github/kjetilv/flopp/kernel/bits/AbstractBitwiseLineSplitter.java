package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.function.Consumer;

@SuppressWarnings("StringTemplateMigration")
abstract class AbstractBitwiseLineSplitter implements Consumer<LineSegment>, SeparatedLine {

    private final Consumer<SeparatedLine> lines;

    private final boolean immutable;

    AbstractBitwiseLineSplitter(Consumer<SeparatedLine> lines, boolean immutable) {
        this.lines = Objects.requireNonNull(lines, "lines");
        this.immutable = immutable;
    }

    protected final void emit() {
        lines.accept(immutable ? immutable() : this);
    }

    protected abstract LineSegment lineSegment();

    protected static final int ALIGNMENT = Math.toIntExact(ValueLayout.JAVA_LONG.byteSize());

    @Override
    public final String toString() {
        return getClass().getSimpleName() +"[" + (lineSegment() == null ? "*" : lineSegment().asString()) + "]";
    }
}
