package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

final class PartitionActionMediator implements BitwisePartitionHandler.Mediator {

    static BitwisePartitionHandler.Mediator create(Partition partition, Shape shape) {
        if (shape == null || !shape.hasOverhead()) {
            return null;
        }
        boolean first = partition.first();
        boolean last = partition.last();
        if (first && last) {
            return new PartitionActionMediator(shape.header(), shape.footer());
        }
        if (first) {
            return new PartitionActionMediator(shape.header(), 0);
        }
        if (last && shape.footer() > 0) {
            return new PartitionActionMediator(0, shape.footer());
        }
        return null;
    }

    private final int header;

    private final int footer;

    private PartitionActionMediator(int header, int footer) {
        this.header = header;
        this.footer = footer;
    }

    @Override
    public BitwisePartitionHandler.Action apply(BitwisePartitionHandler.Action action) {
        Objects.requireNonNull(action, "action");
        if (header > 0 && footer > 0) {
            return new Both(action);
        }
        if (header > 0) {
            return new HeaderOnly(action);
        }
        return new FooterOnly(action);
    }

    private void cycle(
        MemorySegment memorySegment,
        long offset,
        long length,
        Deque<Runnable> deq,
        BitwisePartitionHandler.Action delegate
    ) {
        if (deq.size() == footer) {
            Objects.requireNonNull(deq.pollLast(), "deq.pollLast()").run();
        }
        deq.offerFirst(() -> delegate.line(memorySegment, offset, length));
    }

    @SuppressWarnings("DuplicatedCode")
    private final class HeaderOnly implements BitwisePartitionHandler.Action {

        private int headersLeft = header;

        private final BitwisePartitionHandler.Action delegate;

        private HeaderOnly(BitwisePartitionHandler.Action delegate) {
            this.delegate = Objects.requireNonNull(delegate, "action");
        }

        @Override
        public void line(MemorySegment segment, long offset, long length) {
            if (headersLeft == 0) {
                delegate.line(segment, offset, length);
            } else {
                headersLeft--;
            }
        }

        @Override
        public String toString() {
            boolean h = header > 0;
            return STR."\{getClass().getSimpleName()}[\{h ? STR."\{headersLeft}/\{header}" : ""}]";
        }
    }

    @SuppressWarnings("DuplicatedCode")
    private final class Both implements BitwisePartitionHandler.Action {

        private final BitwisePartitionHandler.Action delegate;

        private final Deque<Runnable> deque = new ArrayDeque<>(footer);

        private int headersLeft = header;

        private Both(BitwisePartitionHandler.Action delegate) {
            this.delegate = Objects.requireNonNull(delegate, "action");
        }

        @Override
        public void line(MemorySegment segment, long offset, long length) {
            if (headersLeft == 0) {
                cycle(segment, offset, length, deque, delegate);
            } else {
                headersLeft--;
            }
        }

        @Override
        public String toString() {
            boolean h = header > 0;
            return STR."\{getClass().getSimpleName()}[\{
                (h ? STR."\{headersLeft}/\{header}" : "") +
                STR."\{h ? " " : ""}q:\{deque.size()}"}]";
        }
    }

    @SuppressWarnings("DuplicatedCode")
    private final class FooterOnly implements BitwisePartitionHandler.Action {

        private final BitwisePartitionHandler.Action delegate;

        private final Deque<Runnable> deque = new ArrayDeque<>(footer);

        private FooterOnly(BitwisePartitionHandler.Action delegate) {
            this.delegate = Objects.requireNonNull(delegate, "action");
        }

        @Override
        public void line(MemorySegment segment, long offset, long length) {
            cycle(segment, offset, length, deque, delegate);
        }

        @Override
        public String toString() {
            boolean h = header > 0;
            return STR."\{getClass().getSimpleName()}[\{STR."\{h ? " " : ""}q:\{deque.size()}"}]";
        }
    }
}
