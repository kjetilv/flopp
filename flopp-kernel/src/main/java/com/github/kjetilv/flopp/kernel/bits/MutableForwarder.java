package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Consumer;

final class MutableForwarder implements BitwisePartitioned.Action, LineSegment {

    private final Consumer<? super LineSegment> action;

    private MemorySegment memorySegment;

    private long startIndex;

    private long endIndex;

    MutableForwarder(Consumer<? super LineSegment> action) {
        this.action = Objects.requireNonNull(action, "action");
    }

    @Override
    public void line(MemorySegment memorySegment, long startIndex, long endIndex) {
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.memorySegment = memorySegment;
        action.accept(this);
    }

    @Override
    public void close() {
        this.startIndex = 0;
        this.endIndex = 0;
        this.memorySegment = null;
    }

    @Override
    public MemorySegment memorySegment() {
        return memorySegment;
    }

    @Override
    public long startIndex() {
        return startIndex;
    }

    @Override
    public long endIndex() {
        return endIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(memorySegment, startIndex, endIndex);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
               obj instanceof LineSegment ls && memorySegment().equals(ls.memorySegment()) &&
               startIndex() == ls.startIndex() &&
               endIndex() == ls.endIndex();
    }

    @Override
    public String toString() {
        return LineSegments.asString(memorySegment, startIndex, endIndex);
    }
}
