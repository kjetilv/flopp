package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.util.function.Consumer;
import java.util.function.Function;

interface LineSplitter extends Function<LineSegment, SeparatedLine>, Consumer<LineSegment> {

    @Override
    default void accept(LineSegment segment) {
        apply(segment);
    }
}
