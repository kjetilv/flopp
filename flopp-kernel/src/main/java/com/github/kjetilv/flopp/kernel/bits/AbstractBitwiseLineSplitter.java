package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;
import java.util.function.Function;

abstract class AbstractBitwiseLineSplitter
    implements Function<LineSegment, SeparatedLine>, Consumer<LineSegment>, SeparatedLine {

    private final Consumer<SeparatedLine> lines;

    private final boolean immutable;

    AbstractBitwiseLineSplitter(Consumer<SeparatedLine> lines, boolean immutable) {
        this.lines = lines == null ? _ -> {} : lines;
        this.immutable = immutable;
    }

    @Override
    public void accept(LineSegment segment) {
        apply(segment);
    }

    protected final SeparatedLine emit(SeparatedLine separatedLine) {
        lines.accept(separatedLine);
        return separatedLine;
    }

    protected SeparatedLine asSeparatedLine() {
        return immutable ? immutableSeparatedLine() : this;
    }

    protected abstract LineSegment lineSegment();

    @Override
    public final String toString() {
        String sub = substring();
        boolean hasSub = sub == null || sub.isBlank();
        return getClass().getSimpleName() + "[" +
               (hasSub ? "" : sub + " ") + "`" + (lineSegment() == null ? "*" : lineSegment().asString()) + "`" +
               "]";
    }

    protected String substring() {
        return null;
    }
}
