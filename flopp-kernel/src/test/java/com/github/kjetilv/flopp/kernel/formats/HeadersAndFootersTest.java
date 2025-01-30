package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.util.CloseableConsumer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class HeadersAndFootersTest {

    @Test
    void testH0f1() {
        assertRange(decor(1, 0), 1, 29);
    }

    @Test
    void testH0f5() {
        assertRange(decor(0, 5), 0, 24);
    }

    @Test
    void testH1f0() {
        assertRange(decor(1, 0), 1, 29);
    }

    @Test
    void testH1f1() {
        assertRange(decor(1, 1), 1, 28);
    }

    @Test
    void testH1f5() {
        assertRange(decor(1, 5), 1, 24);
    }

    @Test
    void testH5f0() {
        assertRange(decor(5, 0), 5, 29);
    }

    @Test
    void testH5f1() {
        assertRange(decor(5, 1), 5, 28);
    }

    @Test
    void testH5f5() {
        assertRange(decor(5, 5), 5, 24);
    }

    private static Shape.Decor decor(int header, int footer) {
        return new Shape.Decor(header, footer);
    }

    private static void assertRange(Shape.Decor hf, int startInclusive, int endExclusive) {
        HeadersAndFooters<Integer> if0 = HeadersAndFooters.create(
            new Partition(0, 3, 0, 10),
            new Shape(200, hf),
            i -> i
        );
        HeadersAndFooters<Integer> if1 = HeadersAndFooters.create(
            new Partition(1, 3, 20, 10),
            new Shape(200, hf),
            i -> i
        );
        HeadersAndFooters<Integer> if2 = HeadersAndFooters.create(
            new Partition(2, 3, 20, 10),
            new Shape(200, hf),
            i -> i
        );

        List<Integer> is = new ArrayList<>();
        CloseableConsumer<Integer> add = is::add;

        try (
            CloseableConsumer<Integer> cc0 = if0 == null ? add : if0.wrap(add);
            CloseableConsumer<Integer> cc1 = if1 == null ? add : if1.wrap(add);
            CloseableConsumer<Integer> cc2 = if2 == null ? add : if2.wrap(add)
        ) {
            for (int i = 0; i < 10; i++) {
                cc0.accept(i);
            }
            for (int i = 10; i < 20; i++) {
                cc1.accept(i);
            }
            for (int i = 20; i < 30; i++) {
                cc2.accept(i);
            }
        }

        assertThat(is)
            .hasSize(endExclusive - startInclusive + 1);
        assertThat(is)
            .containsAll(IntStream.range(startInclusive, endExclusive).boxed()
                .toList());
    }

}