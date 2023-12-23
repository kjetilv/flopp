package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

class SurroundConsumersTest {

    @Test
    void oneByOne() {
        BiConsumer<Consumer<Integer>, Integer> consumer = SurroundConsumers.surround(1, 2);
        List<Integer> foos = new ArrayList<>();
        consumer.accept( foos::add, 1);
        consumer.accept(foos::add, 2);
        consumer.accept(foos::add, 3);
        consumer.accept(foos::add, 4);
        consumer.accept(foos::add, 5);

        assertThat(foos).containsExactly(2, 3);
    }
}
