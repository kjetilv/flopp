package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.util.CloseableConsumer;
import com.github.kjetilv.flopp.kernel.util.Non;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public final class HeadersAndFooters<T> {

    public static <T> HeadersAndFooters<T> create(
        Partition partition,
        com.github.kjetilv.flopp.kernel.Shape shape,
        Function<T, T> immutableCopier
    ) {
        if (shape != null && shape.hasOverhead()) {
            if (partition.single() && shape.hasOverhead()) {
                return new HeadersAndFooters<>(shape.header(), shape.footer(), immutableCopier);
            }
            if (partition.first()) {
                return new HeadersAndFooters<>(shape.header(), 0, immutableCopier);
            }
            if (partition.last() && shape.footer() > 0) {
                return new HeadersAndFooters<>(0, shape.footer(), immutableCopier);
            }
        }
        return null;
    }

    private final int header;

    private final int footer;

    private final Function<T, T> immutableCopier;

    private HeadersAndFooters(int header, int footer, Function<T, T> immutableCopier) {
        this.header = Non.negative(header, "header");
        this.footer = Non.negative(footer, "footer");
        this.immutableCopier = immutableCopier;
    }

    public CloseableConsumer<T> wrap(Consumer<T> consumer) {
        return header == 0 && footer == 0 ? consumer::accept
            : header > 0 && footer == 1 ? new HF1<>(consumer, header, footer, immutableCopier)
                : header > 0 && footer > 0 ? new HF<>(consumer, header, footer, immutableCopier)
                    : header > 0 ? new HO<>(consumer, header)
                        : footer == 1 ? new F1O<>(consumer, immutableCopier)
                            : new FO<>(consumer, footer, immutableCopier);
    }

    private static <T> void cycle(
        T lineSegment,
        Deque<Runnable> deq,
        Consumer<T> delegate,
        int footer,
        Function<T, T> packer
    ) {
        if (deq.size() == footer) {
            deq.pollLast().run();
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

    private static void verifyFooter(Runnable runnable) {
        if (runnable == null) {
            throw new IllegalStateException(
                "Last partition not big enough to hold footer of 1. Increase tail size");
        }
    }

    private static final class HO<T> implements CloseableConsumer<T> {

        private final Consumer<T> delegate;

        private final int header;

        private int headersLeft;

        private HO(Consumer<T> delegate, int header) {
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

    private static final class HF<T> implements CloseableConsumer<T> {

        private final Consumer<T> delegate;

        private final int header;

        private final int footer;

        private final Deque<Runnable> deque;

        private int headersLeft;

        private final Function<T, T> immutableCopier;

        private HF(Consumer<T> delegate, int header, int footer, Function<T, T> immutableCopier) {
            this.delegate = Objects.requireNonNull(delegate, "action");
            this.header = Non.negativeOrZero(header, "header");
            this.footer = Non.negativeOrZero(footer, "footer");
            this.deque = new ArrayDeque<>(footer);
            this.headersLeft = header;
            this.immutableCopier = immutableCopier;
        }

        @Override
        public void accept(T lineSegment) {
            if (headersLeft == 0) {
                cycle(lineSegment, deque, delegate, footer, immutableCopier);
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

    private static final class HF1<T> implements CloseableConsumer<T> {

        private final Consumer<T> delegate;

        private final int header;

        private int headersLeft;

        private final Function<T, T> immutableCopier;

        private Runnable queued;

        private HF1(Consumer<T> delegate, int header, int footer, Function<T, T> immutableCopier) {
            this.delegate = Objects.requireNonNull(delegate, "action");
            this.header = Non.negativeOrZero(header, "header");
            this.headersLeft = header;
            this.immutableCopier = immutableCopier;
        }

        @Override
        public void accept(T lineSegment) {
            if (headersLeft == 0) {
                Runnable runnable = queued;
                if (runnable != null) {
                    queued = null;
                    runnable.run();
                }
                T immutable = immutableCopier.apply(lineSegment);
                queued = () -> delegate.accept(immutable);
            } else {
                headersLeft--;
            }
        }

        @Override
        public void close() {
            verifyHeader(headersLeft, header);
            verifyFooter(queued);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" +
                   headersLeft + "/" + header + " q:" + (queued == null ? 0 : 1) +
                   "]";
        }
    }

    private static final class FO<T> implements CloseableConsumer<T> {

        private final Consumer<T> delegate;

        private final Deque<Runnable> queued;

        private final int footer;

        private final Function<T, T> immutableCopier;

        private FO(Consumer<T> delegate, int footer, Function<T, T> immutableCopier) {
            this.delegate = Objects.requireNonNull(delegate, "action");
            this.footer = Non.negativeOrZero(footer, "footer");
            this.immutableCopier = immutableCopier;
            this.queued = new ArrayDeque<>(this.footer);
        }

        @Override
        public void accept(T lineSegment) {
            cycle(lineSegment, queued, delegate, footer, immutableCopier);
        }

        @Override
        public void close() {
            verifyFooter(queued, footer);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[q:" + queued.size() + "]";
        }
    }

    private static final class F1O<T> implements CloseableConsumer<T> {

        private final Consumer<T> delegate;

        private final Function<T, T> immutableCopier;

        private Runnable queued;

        private F1O(Consumer<T> delegate, Function<T, T> immutableCopier) {
            this.delegate = Objects.requireNonNull(delegate, "action");
            this.immutableCopier = immutableCopier;
        }

        @Override
        public void accept(T lineSegment) {
            Runnable runnable = queued;
            if (runnable != null) {
                queued = null;
                runnable.run();
            }
            T immutable = immutableCopier.apply(lineSegment);
            queued = () -> delegate.accept(immutable);
        }

        @Override
        public void close() {
            verifyFooter(queued);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[q:" + (queued == null ? 0 : 1) + "]";
        }
    }
}
