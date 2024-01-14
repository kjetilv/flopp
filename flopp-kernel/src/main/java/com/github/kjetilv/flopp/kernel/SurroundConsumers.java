package com.github.kjetilv.flopp.kernel;

import java.util.Deque;
import java.util.LinkedList;
import java.util.function.Consumer;

final class SurroundConsumers {

    static <T> SurroundConsumer<T> surround(int header, int footer) {
        return header == 0 && footer == 0
            ? Consumer::accept
            : new Default<>(header, footer);
    }

    private SurroundConsumers() {
    }

    private static final class Default<T> implements SurroundConsumer<T> {

        private final int header;

        private final int footer;

        private final Deque<T> deque;

        private int headersLeft;

        private Default(int header, int footer) {
            this.header = Non.negative(header, "header'");
            this.footer = Non.negative(footer, "footer");
            this.headersLeft = this.header;
            this.deque = footer > 0 ? new LinkedList<>() : null;
        }

        @Override
        public void accept(Consumer<? super T> consumer, T t) {
            if (headersLeft > 0) {
                headersLeft--;
                return;
            }
            if (deque == null) {
                consumer.accept(t);
                return;
            }
            if (deque.size() < footer) {
                deque.offerFirst(t);
                return;
            }
            T delayed = deque.pollLast();
            consumer.accept(delayed);
            deque.offerFirst(t);
        }

        @Override
        public String toString() {
            boolean h = header > 0;
            boolean f = deque != null;
            return STR."\{getClass().getSimpleName()}[\{
                h || f ?
                    (h ? STR."\{headersLeft}/\{header}" : "") +
                    (f ? STR."\{h ? " " : ""}q:\{deque.size()}" : "")
                    : ""}]";
        }
    }
}
