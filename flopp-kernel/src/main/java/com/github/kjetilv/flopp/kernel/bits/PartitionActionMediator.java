package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

@SuppressWarnings("StringTemplateMigration")
final class PartitionActionMediator implements BitwisePartitionHandler.Mediator {

    static BitwisePartitionHandler.Mediator create(Partition partition, Shape shape) {
        if (shape != null && shape.hasOverhead()) {
            if (partition.single() && shape.hasOverhead()) {
                return new PartitionActionMediator(shape.header(), shape.footer());
            }
            if (partition.first()) {
                return new PartitionActionMediator(shape.header(), 0);
            }
            if (partition.last() && shape.footer() > 0) {
                return new PartitionActionMediator(0, shape.footer());
            }
        }
        return null;
    }

    private final int header;

    private final int footer;

    private PartitionActionMediator(int header, int footer) {
        this.header = Non.negative(header, "header");
        this.footer = Non.negative(footer, "footer");
    }

    @Override
    public BitwisePartitioned.Action mediate(BitwisePartitioned.Action action) {
        Objects.requireNonNull(action, "action");
        return header > 0 && footer > 0 ? new HeaderAndFooter(action, header, footer)
            : header > 0 ? new HeaderOnly(action, header)
                : new FooterOnly(action, footer);
    }

    private static void cycle(
        MemorySegment memorySegment,
        long startIndex,
        long endIndex,
        Deque<Runnable> deq,
        BitwisePartitioned.Action delegate,
        int footer
    ) {
        if (deq.size() == footer) {
            Objects.requireNonNull(deq.pollLast(), "deq.pollLast()").run();
        }
        deq.offerFirst(() -> delegate.line(memorySegment, startIndex, endIndex));
    }

    private static void verifyHeader(int headersLeft, int header) {
        if (headersLeft > 0) {
            throw new IllegalStateException(
                "First partition not big enough to hold header of " + header + ". Lower partition count");
        }
    }

    private static void verifyFooter(Deque<Runnable> deq, int footer) {
        if (deq.size() < footer) {
            throw new IllegalStateException(
                "Last partition not big enough to hold footer of " + footer + ". Increase tail size");
        }
    }

    private static final class HeaderOnly implements BitwisePartitioned.Action {

        private final BitwisePartitioned.Action delegate;

        private final int header;

        private int headersLeft;

        private HeaderOnly(BitwisePartitioned.Action delegate, int header) {
            this.delegate = Objects.requireNonNull(delegate, "action");
            this.header = header;
            this.headersLeft = header;
        }

        @Override
        public void line(MemorySegment segment, long startIndex, long endIndex) {
            if (headersLeft == 0) {
                delegate.line(segment, startIndex, endIndex);
            } else {
                headersLeft--;
            }
        }

        @Override
        public void close() {
            delegate.close();
            verifyHeader(headersLeft, header);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + headersLeft + "/" + header + "]";
        }
    }

    private static final class HeaderAndFooter implements BitwisePartitioned.Action {

        private final BitwisePartitioned.Action delegate;

        private final int header;

        private final int footer;

        private final Deque<Runnable> deque;

        private int headersLeft;

        private HeaderAndFooter(BitwisePartitioned.Action delegate, int header, int footer) {
            this.delegate = Objects.requireNonNull(delegate, "action");
            this.header = Non.negativeOrZero(header, "header");
            this.footer = Non.negativeOrZero(footer, "footer");
            this.deque = new ArrayDeque<>(footer);
            this.headersLeft = header;
        }

        @Override
        public void line(MemorySegment segment, long startIndex, long endIndex) {
            if (headersLeft == 0) {
                cycle(segment, startIndex, endIndex, deque, delegate, footer);
            } else {
                headersLeft--;
            }
        }

        @Override
        public void close() {
            delegate.close();
            verifyHeader(headersLeft, header);
            verifyFooter(deque, footer);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + headersLeft + "/" + header + " q:" + deque.size() + "]";
        }
    }

    private static final class FooterOnly implements BitwisePartitioned.Action {

        private final BitwisePartitioned.Action delegate;

        private final Deque<Runnable> deque;

        private final int footer;

        private FooterOnly(BitwisePartitioned.Action delegate, int footer) {
            this.delegate = Objects.requireNonNull(delegate, "action");
            this.footer = Non.negativeOrZero(footer, "footer");
            this.deque = new ArrayDeque<>(this.footer);
        }

        @Override
        public void line(MemorySegment segment, long startIndex, long endIndex) {
            cycle(segment, startIndex, endIndex, deque, delegate, footer);
        }

        @Override
        public void close() {
            delegate.close();
            verifyFooter(deque, footer);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[q:" + deque.size() + "]";
        }
    }
}
