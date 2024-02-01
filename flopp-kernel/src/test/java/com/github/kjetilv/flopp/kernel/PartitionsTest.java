package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionsTest {

    @Test
    public void testShortTail() {
        List<Partition> partitions = Partitioning.create(3, 80).of(2000);
        assertSizes(partitions, 640L, 640L, 640L, 80L);
    }

    @Test
    public void testShortTailAligned() {
        List<Partition> partitions = Partitioning.create(3, 80).of(1996);
        assertSizes(partitions, 640L, 640L, 632L, 84L);
    }

    @Test
    public void test692() {
        List<Partition> partitions = Partitioning.create(6).of(104);
        assertSizes(partitions, 24L, 16L, 16L, 16L, 16L, 16L);
    }

    @Test
    public void testLongAligned() {
        List<Partition> partitions = Partitioning.create(3).of(65);
        assertSizes(partitions, 24L, 24L, 17L);
    }

    @Test
    public void testLongAlignedShort() {
        List<Partition> partitions = Partitioning.create(3).of(52);
        assertSizes(partitions, 16L, 16L, 20L);
    }

    private static void assertSizes(List<Partition> partitions, Long... expectedSizes) {
        assertEquals(partitions.size(), expectedSizes.length);
        for (int i = 0; i < partitions.size(); i++) {
            assertEquals(
                expectedSizes[i],
                partitions.get(i).count(),
                STR."Partition had wrong size #\{i}: \{partitions.get(i)}"
            );
        }
        assertThat(partitions.getFirst().first()).isTrue();
        assertThat(partitions.getLast().last()).isTrue();
    }
}
