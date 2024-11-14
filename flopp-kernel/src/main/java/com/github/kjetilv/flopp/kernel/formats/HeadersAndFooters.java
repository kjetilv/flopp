package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.util.CloseableConsumer;
import com.github.kjetilv.flopp.kernel.util.Non;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public final class HeadersAndFooters<T> implements Function<Consumer<T>, CloseableConsumer<T>> {

    public static <T> Function<Consumer<T>, CloseableConsumer<T>> headersAndFooters(
        Partition partition,
        com.github.kjetilv.flopp.kernel.Shape shape,
        Function<T, T> packer
    ) {
        if (shape != null && shape.hasOverhead()) {
            if (partition.single() && shape.hasOverhead()) {
                return new HeadersAndFooters<>(shape.header(), shape.footer(), packer);
            }
            if (partition.first()) {
                return new HeadersAndFooters<>(shape.header(), 0, packer);
            }
            if (partition.last() && shape.footer() > 0) {
                return new HeadersAndFooters<>(0, shape.footer(), packer);
            }
        }
        return null;
    }

    private final int header;

    private final int footer;

    private final Function<T, T> packer;

    private HeadersAndFooters(int header, int footer, Function<T, T> packer) {
        this.header = Non.negative(header, "header");
        this.footer = Non.negative(footer, "footer");
        this.packer = packer;
    }

    @Override
    public CloseableConsumer<T> apply(Consumer<T> consumer) {
        return header == 0 && footer == 0 ? consumer::accept
            : header > 0 && footer > 0 ? new HeaderAndFooter<>(consumer, header, footer, packer)
                : header > 0 ? new HeaderOnly<>(consumer, header)
                    : new FooterOnly<>(consumer, footer, packer);
    }

    private static <T> void cycle(
        T lineSegment,
        Deque<Runnable> deq,
        Consumer<T> delegate,
        int footer,
        Function<T, T> packer
    ) {
        if (deq.size() == footer) {
            Objects.requireNonNull(deq.pollLast(), "deq.pollLast()").run();
        }
        T immutable = packer.apply(lineSegment);
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

    private static final class HeaderOnly<T> implements CloseableConsumer<T> {

        private final Consumer<T> delegate;

        private final int header;

        private int headersLeft;

        private HeaderOnly(Consumer<T> delegate, int header) {
            this.delegate = Objects.requireNonNull(delegate, "action");
            this.header = header;
            this.headersLeft = header;
        }

        @Override
        public void accept(T lineSegment) {
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

    private static final class HeaderAndFooter<T> implements CloseableConsumer<T> {

        private final Consumer<T> delegate;

        private final int header;

        private final int footer;

        private final Deque<Runnable> deque;

        private int headersLeft;

        private final Function<T, T> packer;

        private HeaderAndFooter(Consumer<T> delegate, int header, int footer, Function<T, T> packer) {
            this.delegate = Objects.requireNonNull(delegate, "action");
            this.header = Non.negativeOrZero(header, "header");
            this.footer = Non.negativeOrZero(footer, "footer");
            this.deque = new ArrayDeque<>(footer);
            this.headersLeft = header;
            this.packer = packer;
        }

        @Override
        public void accept(T lineSegment) {
            if (headersLeft == 0) {
                cycle(lineSegment, deque, delegate, footer, packer);
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

    private static final class FooterOnly<T> implements CloseableConsumer<T> {

        private final Consumer<T> delegate;

        private final Deque<Runnable> deque;

        private final int footer;

        private final Function<T, T> packer;

        private FooterOnly(Consumer<T> delegate, int footer, Function<T, T> packer) {
            this.delegate = Objects.requireNonNull(delegate, "action");
            this.footer = Non.negativeOrZero(footer, "footer");
            this.packer = packer;
            this.deque = new ArrayDeque<>(this.footer);
        }

        @Override
        public void accept(T lineSegment) {
            cycle(lineSegment, deque, delegate, footer, packer);
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
