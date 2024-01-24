package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionsTest {

    @Test
    public void testShortTail() {
        List<Partition> partitions = Partitioning.longAligned(3, 80).of(2000);
        assertSizes(partitions, 84 * 8L, 83 * 8L, 83 * 8L);
    }

    @Test
    public void testShortTailAligned() {
        List<Partition> partitions = Partitioning.longAligned(3, 80).of(1996);
        assertSizes(partitions, 640L, 640L, 632L, 84L);
    }

    @Test
    public void test1418() {
        List<Partition> partitions = Partitioning.count(4).of(18);
        assertSizes(partitions, 5L, 5L, 4L, 4L);
    }

    @Test
    public void test692() {
        List<Partition> partitions = Partitioning.longAligned(6).of(104);
        assertSizes(partitions, 24L, 16L, 16L, 16L, 16L, 16L);
    }

    @Test
    public void testLongAligned() {
        List<Partition> partitions = Partitioning.longAligned(3).of(65);
        assertSizes(partitions, 24L, 24L, 17L);
    }

    @Test
    public void testLongAlignedShort() {
        List<Partition> partitions = Partitioning.longAligned(3).of(52);
        assertSizes(partitions, 16L, 16L, 20L);
    }

    @Test
    public void test12zero() {
        List<Partition> partitions = Partitioning.count(4).of(12);
        assertSizes(partitions, 3L, 3L, 3L, 3L);
    }

    @Test
    public void test12one() {
        List<Partition> partitions = Partitioning.count(4).of(13);
        assertSizes(partitions, 4L, 3L, 3L, 3L);
    }

    @Test
    public void test1216() {
        List<Partition> partitions = Partitioning.count(3).of(16);
        assertSizes(partitions, 6L, 5L, 5L);
    }

    @Test
    public void testALot() {
        List<Partition> partitions = Partitioning.count(4).of(100_004);
        assertSizes(partitions, 25_001L, 25_001L, 25_001L, 25_001L);
    }

    @Test
    public void testALotPlus1() {
        List<Partition> partitions = Partitioning.count(4).of(100_005);
        assertSizes(partitions, 25_002L, 25_001L, 25_001L, 25_001L);
    }

    @Test
    public void testALotMin1() {
        List<Partition> partitions = Partitioning.count(4).of(100_003);
        assertSizes(partitions, 25_001L, 25_001L, 25_001L, 25_000L);
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
