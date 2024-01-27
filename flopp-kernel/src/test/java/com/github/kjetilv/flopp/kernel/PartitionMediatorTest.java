package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionMediatorTest {

    @Test
    void oneByOne() {
        Mediator<Integer> mediator =
            PartitionMediator.create(new Partition(0, 1, 0, 100), Shape.size(100).header(1, 2), null);
        List<Integer> foos = new ArrayList<>();
        Consumer<Integer> consumer = (Consumer<Integer>) mediator.apply(foos::add);
        consumer.accept(1);
        consumer.accept(2);
        consumer.accept(3);
        consumer.accept(4);
        consumer.accept(5);

        assertThat(foos).containsExactly(2, 3);
    }
}
