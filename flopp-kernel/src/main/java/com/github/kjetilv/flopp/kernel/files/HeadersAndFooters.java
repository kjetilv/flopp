package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.util.Non;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

final class HeadersAndFooters implements Function<Consumer<LineSegment>, PartitionedPath.Action> {

    static Function<Consumer<LineSegment>, PartitionedPath.Action> headersAndFooters(
        Partition partition,
        Shape shape
    ) {
        if (shape != null && shape.hasOverhead()) {
            if (partition.single() && shape.hasOverhead()) {
                return new HeadersAndFooters(shape.header(), shape.footer());
            }
            if (partition.first()) {
                return new HeadersAndFooters(shape.header(), 0);
            }
            if (partition.last() && shape.footer() > 0) {
                return new HeadersAndFooters(0, shape.footer());
            }
        }
        return null;
    }

    private final int header;

    private final int footer;

    private HeadersAndFooters(int header, int footer) {
        this.header = Non.negative(header, "header");
        this.footer = Non.negative(footer, "footer");
    }

    @Override
    public PartitionedPath.Action apply(Consumer<LineSegment> consumer) {
        return header == 0 && footer == 0 ? consumer::accept
            : header > 0 && footer > 0 ? new HeaderAndFooter(consumer, header, footer)
                : header > 0 ? new HeaderOnly(consumer, header)
                    : new FooterOnly(consumer, footer);
    }

    private static void cycle(
        LineSegment lineSegment,
        Deque<Runnable> deq,
        Consumer<LineSegment> delegate,
        int footer
    ) {
        if (deq.size() == footer) {
            Objects.requireNonNull(deq.pollLast(), "deq.pollLast()").run();
        }
        LineSegment immutable = lineSegment.immutable();
        deq.offerFirst(() -> delegate.accept(immutable));
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

    private static final class HeaderOnly implements PartitionedPath.Action {

        private final Consumer<LineSegment> delegate;

        private final int header;

        private int headersLeft;

        private HeaderOnly(Consumer<LineSegment> delegate, int header) {
            this.delegate = Objects.requireNonNull(delegate, "action");
            this.header = header;
            this.headersLeft = header;
        }

        @Override
        public void accept(LineSegment lineSegment) {
            if (headersLeft == 0) {
                delegate.accept(lineSegment);
            } else {
                headersLeft--;
            }
        }

        @Override
        public void close() {
            verifyHeader(headersLeft, header);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + headersLeft + "/" + header + "]";
        }
    }

    private static final class HeaderAndFooter implements PartitionedPath.Action {

        private final Consumer<LineSegment> delegate;

        private final int header;

        private final int footer;

        private final Deque<Runnable> deque;

        private int headersLeft;

        private HeaderAndFooter(Consumer<LineSegment> delegate, int header, int footer) {
            this.delegate = Objects.requireNonNull(delegate, "action");
            this.header = Non.negativeOrZero(header, "header");
            this.footer = Non.negativeOrZero(footer, "footer");
            this.deque = new ArrayDeque<>(footer);
            this.headersLeft = header;
        }

        @Override
        public void accept(LineSegment lineSegment) {
            if (headersLeft == 0) {
                cycle(lineSegment, deque, delegate, footer);
            } else {
                headersLeft--;
            }
        }

        @Override
        public void close() {
            verifyHeader(headersLeft, header);
            verifyFooter(deque, footer);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + headersLeft + "/" + header + " q:" + deque.size() + "]";
        }
    }

    private static final class FooterOnly implements PartitionedPath.Action {

        private final Consumer<LineSegment> delegate;

        private final Deque<Runnable> deque;

        private final int footer;

        private FooterOnly(Consumer<LineSegment> delegate, int footer) {
            this.delegate = Objects.requireNonNull(delegate, "action");
            this.footer = Non.negativeOrZero(footer, "footer");
            this.deque = new ArrayDeque<>(this.footer);
        }

        @Override
        public void accept(LineSegment lineSegment) {
            cycle(lineSegment, deque, delegate, footer);
        }

        @Override
        public void close() {
            verifyFooter(deque, footer);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[q:" + deque.size() + "]";
        }
    }
}
