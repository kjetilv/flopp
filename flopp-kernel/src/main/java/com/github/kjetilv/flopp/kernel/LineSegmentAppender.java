package com.github.kjetilv.flopp.kernel;

public interface LineSegmentAppender {

    static LineSegmentAppender create(LineSegment target) {
        return new App(target);
    }

    default void accept(long data) {
        accept(data, MemorySegments.ALIGNMENT_INT);
    }

    void accept(long data, int length);

    final class App implements LineSegmentAppender {

        private final LineSegment target;

        private long pos;

        public App(LineSegment target) {
            this.target = target;
            this.pos = target.startIndex();
        }

        @Override
        public void accept(long data, int length) {
            try {
                target.write(pos, data, length);
            } finally {
                pos += length;
            }
        }
    }
}
