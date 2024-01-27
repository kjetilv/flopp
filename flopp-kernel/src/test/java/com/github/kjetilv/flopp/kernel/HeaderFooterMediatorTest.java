package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

class HeaderFooterMediatorTest {

    @Test
    void oneByOne() {
        HeaderFooterMediator<Integer> med = new HeaderFooterMediator<>(1, 2, null);
        List<Integer> foos = new ArrayList<>();
        Consumer<Integer> consumer = (Consumer<Integer>) med.apply(foos::add);
        consumer.accept(1);
        consumer.accept(2);
        consumer.accept(3);
        consumer.accept(4);
        consumer.accept(5);

        assertThat(foos).containsExactly(2, 3);
    }
}
