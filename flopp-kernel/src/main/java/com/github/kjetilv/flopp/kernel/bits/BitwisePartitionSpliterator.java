package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitionHandler.MiddleMan;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitioned.Action;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

final class BitwisePartitionSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final MiddleMan<BitwisePartitioned.Action> middleMan;

    private final Supplier<BitwisePartitionSpliterator> next;

    private MemorySegment segment;

    private final long logicalSize;

    private long underlyingSize;

    BitwisePartitionSpliterator(
        Partition partition,
        MemorySegment segment,
        long logicalSize,
        MiddleMan<BitwisePartitioned.Action> middleMan,
        Supplier<BitwisePartitionSpliterator> next
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.partition = Objects.requireNonNull(partition, "partition");
        this.logicalSize = logicalSize;
        this.middleMan = middleMan == null
            ? action -> action
            : middleMan;
        this.next = next;
        adopt(segment);
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
            Action delegate = new MutableForwarder(action);
            handler(middleMan.intercept(delegate)).run();
            return false;
        } catch (Exception e) {
            throw new IllegalStateException(this + " failed: " + action, e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[@" + partition + "]";
    }

    private BitwisePartitionHandler handler(Action action) {
        return new BitwisePartitionHandler(
            partition,
            segment,
            logicalSize,
            action,
            next == null
                ? null
                : () -> next.get().handler(action)
        );
    }

    private void adopt(MemorySegment segment) {
        this.segment = segment;
        this.underlyingSize = segment.byteSize();
    }

    private final class MutableForwarder implements Action, LineSegment {

        private final Consumer<? super LineSegment> action;

        private long startIndex;

        private long endIndex;

        MutableForwarder(Consumer<? super LineSegment> action) {
            this.action = action;
        }

        @Override
        public void line(long startIndex, long endIndex) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            action.accept(this);
        }

        @Override
        public void line(MemorySegment memorySegment, long startIndex, long endIndex) {
            adopt(memorySegment);
            line(startIndex, endIndex);
        }

        @Override
        public MemorySegment memorySegment() {
            return segment;
        }

        @Override
        public long underlyingSize() {
            return underlyingSize;
        }

        @Override
        public String asString(Charset charset) {
            return asString(null, charset);
        }

        @Override
        public String asString(byte[] buffer, Charset charset) {
            return MemorySegments.fromLongsWithinBounds(segment, startIndex, endIndex, buffer, charset);
        }

        @Override
        public long length() {
            return endIndex - startIndex;
        }

        @Override
        public long headStart() {
            return startIndex % ALIGNMENT;
        }

        @Override
        public boolean isAlignedAtStart() {
            return startIndex % ALIGNMENT == 0L;
        }

        @Override
        public boolean isAlignedAtEnd() {
            return endIndex % ALIGNMENT == 0;
        }

        @Override
        public long head(long head) {
            return segment.get(JAVA_LONG, startIndex - startIndex % ALIGNMENT) >> head * ALIGNMENT;
        }

        @Override
        public long longNo(int longNo) {
            return segment.get(JAVA_LONG, startIndex - startIndex % ALIGNMENT + longNo * ALIGNMENT);
        }

        @Override
        public long bytesAt(long offset, long count) {
            return MemorySegments.bytesAt(memorySegment(), startIndex + offset, count);
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
        public String toString() {
            return startIndex() + "+" + length() + ":" + segment;
        }
    }
}
