package com.github.kjetilv.flopp.kernel;

import java.util.function.Consumer;
import java.util.function.Function;

public interface LineSplitter extends Function<LineSegment, SeparatedLine>, Consumer<LineSegment> {

    @Override
    default void accept(LineSegment segment) {
        apply(segment);
    }
}
