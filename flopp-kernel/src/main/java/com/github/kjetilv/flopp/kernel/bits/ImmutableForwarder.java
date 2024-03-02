package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.util.function.Consumer;

final class ImmutableForwarder implements BitwisePartitioned.Action {

    private final Consumer<? super LineSegment> action;

    ImmutableForwarder(Consumer<? super LineSegment> action) {
        this.action = action;
    }

    @Override
    public void line(MemorySegment segment, long startIndex, long endIndex) {
        action.accept(LineSegments.of(segment, startIndex, endIndex));
    }
}
