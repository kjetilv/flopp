package com.github.kjetilv.flopp;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SurroundConsumerTest {

    @Test
    void oneByOne() {
        SurroundConsumer<Integer> consumer = new SurroundConsumers.DefaultSurroundConsumer<>(1, 2);
        List<Integer> foos = new ArrayList<>();
        consumer.accept( foos::add, 1);
        consumer.accept(foos::add, 2);
        consumer.accept(foos::add, 3);
        consumer.accept(foos::add, 4);
        consumer.accept(foos::add, 5);

        assertThat(foos).containsExactly(2, 3);
    }
}
