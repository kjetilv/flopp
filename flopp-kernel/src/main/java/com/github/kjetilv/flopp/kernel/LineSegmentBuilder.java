package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;

public interface LineSegmentBuilder extends LineSegment {

    static LineSegmentBuilder create(LineSegment vessel) {
        return new App(vessel);
    }

    default void accept(long data) {
        accept(data, MemorySegments.ALIGNMENT_INT);
    }

    void accept(long data, int length);

    default LineSegment build() {
        return this;
    }

    void accept(LineSegment lineSegment);

    final class App implements LineSegmentBuilder, LineSegment {

        private final LineSegment vessel;

        private final long startIndex;

        private long endIndex;

        public App(LineSegment vessel) {
            this.vessel = vessel;
            this.startIndex = vessel.startIndex();
            this.endIndex = vessel.startIndex();
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
        public MemorySegment memorySegment() {
            return vessel.memorySegment();
        }

        @Override
        public void accept(long data, int length) {
            try {
                vessel.write(endIndex, data, length);
            } finally {
                endIndex += length;
            }
        }

        @Override
        public void accept(LineSegment ls) {
            long length = ls.length();
            MemorySegments.copyBytes(ls.memorySegment(), vessel.memorySegment(), endIndex, length);
            endIndex += length;
        }
    }
}
