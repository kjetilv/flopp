package com.github.kjetilv.flopp.kernel.partitions;

import com.github.kjetilv.flopp.kernel.Partitions;
import com.github.kjetilv.flopp.kernel.TailShards;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionsTest {

    @Test
    void testShortTail() {
        Partitions partitions = Partitionings.create(3, 80).of(2000);
        assertSizes(partitions, 640L, 640L, 640L, 80L);
    }

    @Test
    void testShortTailAligned() {
        Partitions partitions = Partitionings.create(3, 80).of(1996);
        assertSizes(partitions, 632L, 640L, 640L, 84L);
    }

    @Test
    void test692() {
        Partitions partitions = Partitionings.create(6).of(104);
        assertSizes(partitions, 16L, 16L, 16L, 16L, 16L, 24L);
    }

    @Test
    void testLongAligned() {
        Partitions partitions = Partitionings.create(3).of(65);
        assertSizes(partitions, 16L, 24L, 25L);
    }

    @Test
    void testLongAlignedShort() {
        Partitions partitions = Partitionings.create(3).of(52);
        assertSizes(partitions, 16L, 16L, 20L);
    }

    @Test
    void fragment() {
        int cpus = Runtime.getRuntime().availableProcessors();
        Partitions partitions = Partitionings.create(cpus, 250)
            .fragment(new TailShards(cpus * 3, 1, 2, 3))
            .of(1_000_000_000);
        System.out.println(partitions);
    }

    private static void assertSizes(Partitions partitions, Long... expectedSizes) {
        assertEquals(partitions.size(), expectedSizes.length);
        for (int i = 0; i < partitions.size(); i++) {
            assertEquals(
                expectedSizes[i],
                partitions.get(i).length(),
                "Partition had wrong size #" + i + ":" + partitions.get(i)
            );
        }
        assertThat(partitions.getFirst().first()).isTrue();
        assertThat(partitions.getLast().last()).isTrue();
    }
}
