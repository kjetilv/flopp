package com.github.kjetilv.flopp.kernel;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public final class HeaderFooterMediator<T> implements Mediator<T> {

    private final int header;

    private final int footer;

    private final Function<T, T> copy;

    public HeaderFooterMediator(int header) {
        this(header, 0, null);
    }

    public HeaderFooterMediator(int header, int footer) {
        this(header, footer, null);
    }

    public HeaderFooterMediator(int footer, Function<T, T> copy) {
        this(0, footer, copy);
    }

    public HeaderFooterMediator(int header, int footer, Function<T, T> copy) {
        this.header = header;
        this.footer = footer;
        this.copy = footer > 0 && copy != null ? copy
            : footer > 0 ? Function.identity()
                : null;
    }

    @Override
    public Consumer<? super T> apply(Consumer<? super T> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        return header == 0 && footer == 0 ? consumer
            : header > 0 && footer > 0 ? new Both(consumer)
                : header > 0 ? new HeaderOnly(consumer)
                    : new FooterOnly(consumer);
    }

    @SuppressWarnings("DuplicatedCode")
    private final class HeaderOnly implements Consumer<T> {

        private int headersLeft = header;

        private final Consumer<? super T> consumer;

        private HeaderOnly(Consumer<? super T> consumer) {
            this.consumer = consumer;
        }

        public void accept(T t) {
            if (headersLeft > 0) {
                headersLeft--;
                return;
            }
            consumer.accept(t);
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

        private final Deque<T> deque = new ArrayDeque<>();

        private int headersLeft = header;

        private Both(Consumer<? super T> consumer) {
            this.consumer = consumer;
        }

        public void accept(T t) {
            if (headersLeft > 0) {
                headersLeft--;
                return;
            }
            if (deque.size() < footer) {
                deque.offerFirst(copy.apply(t));
                return;
            }
            T delayed = deque.pollLast();
            consumer.accept(delayed);
            deque.offerFirst(t);
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
            if (deque.size() < footer) {
                deque.offerFirst(copy.apply(t));
                return;
            }
            T delayed = deque.pollLast();
            consumer.accept(delayed);
            deque.offerFirst(t);
        }

        @Override
        public String toString() {
            boolean h = header > 0;
            return STR."\{getClass().getSimpleName()}[\{STR."\{h ? " " : ""}q:\{deque.size()}"}]";
        }
    }
}
