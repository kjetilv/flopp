package com.github.kjetilv.flopp.kernel;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public final class PartitionMediator<T> implements Mediator<T> {

    public static <T> Mediator<T> create(
        Partition partition,
        Shape shape,
        Function<T, T> copier
    ) {
        if (shape == null || !shape.hasOverhead()) {
            return null;
        }
        boolean first = partition.first();
        boolean last = partition.last();
        if (first && last) {
            return new PartitionMediator<>(shape.header(), shape.footer(), copier);
        }
        if (first) {
            return new PartitionMediator<>(shape.header(), 0, null);
        }
        if (last && shape.footer() > 0) {
            return new PartitionMediator<>(0, shape.footer(), copier);
        }
        return null;
    }

    private final int header;

    private final int footer;

    private final Function<T, T> copy;

    private PartitionMediator(int header, int footer, Function<T, T> copy) {
        this.header = header;
        this.footer = footer;
        this.copy = footer > 0 ? copy : null;
    }

    @Override
    public Consumer<? super T> apply(Consumer<? super T> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (header > 0 && footer > 0) {
            return new Both(consumer);
        }
        if (header > 0) {
            return new HeaderOnly(consumer);
        }
        return new FooterOnly(consumer);
    }

    private T copyOf(T t) {
        return copy == null ? t : copy.apply(t);
    }

    private void cycle(Consumer<? super T> con, Deque<T> deq, T t) {
        if (deq.size() == footer) {
            T delayed = deq.pollLast();
            con.accept(delayed);
        }
        deq.offerFirst(copyOf(t));
    }

    @SuppressWarnings("DuplicatedCode")
    private final class HeaderOnly implements Consumer<T> {

        private int headersLeft = header;

        private final Consumer<? super T> consumer;

        private HeaderOnly(Consumer<? super T> consumer) {
            this.consumer = consumer;
        }

        public void accept(T t) {
            if (headersLeft == 0) {
                consumer.accept(t);
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
    private final class Both implements Consumer<T> {

        private final Consumer<? super T> consumer;

        private final Deque<T> deque = new ArrayDeque<>(footer);

        private int headersLeft = header;

        private Both(Consumer<? super T> consumer) {
            this.consumer = consumer;
        }

        public void accept(T t) {
            if (headersLeft == 0) {
                cycle(consumer, deque, t);
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
    private final class FooterOnly implements Consumer<T> {

        private final Consumer<? super T> consumer;

        private final Deque<T> deque = new ArrayDeque<>(footer);

        private FooterOnly(Consumer<? super T> consumer) {
            this.consumer = consumer;
        }

        public void accept(T t) {
            cycle(consumer, deque, t);
        }

        @Override
        public String toString() {
            boolean h = header > 0;
            return STR."\{getClass().getSimpleName()}[\{STR."\{h ? " " : ""}q:\{deque.size()}"}]";
        }
    }
}
