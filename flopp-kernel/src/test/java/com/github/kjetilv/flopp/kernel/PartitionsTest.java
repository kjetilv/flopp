package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionsTest {

    @Test
    public void test1418() {
        List<Partition> partitions = Partition.partitions(18, 4);
        assertSizes(partitions, 5L, 5L, 4L, 4L);
    }

    @Test
    public void test12zero() {
        List<Partition> partitions = Partition.partitions(12, 4);
        assertSizes(partitions, 3L, 3L, 3L, 3L);
    }

    @Test
    public void test12one() {
        List<Partition> partitions = Partition.partitions(
            13,
            4
        );
        assertSizes(partitions, 4L, 3L, 3L, 3L);
    }

    @Test
    public void test1216() {
        List<Partition> partitions = Partition.partitions(16, 3);
        assertSizes(partitions, 6L, 5L, 5L);
    }

    @Test
    public void testALot() {
        List<Partition> partitions = Partition.partitions(100_004, 4);
        assertSizes(partitions, 25_001L, 25_001L, 25_001L, 25_001L);
    }

    @Test
    public void testALotPlus1() {
        List<Partition> partitions = Partition.partitions(100_005, 4);
        assertSizes(partitions, 25_002L, 25_001L, 25_001L, 25_001L);
    }

    @Test
    public void testALotMin1() {
        List<Partition> partitions = Partition.partitions(100_003, 4);
        assertSizes(partitions, 25_001L, 25_001L, 25_001L, 25_000L);
    }

    private static void assertSizes(List<Partition> partitions, Long... expectedSizes) {
        assertEquals(partitions.size(), expectedSizes.length);
        for (int i = 0; i < partitions.size(); i++) {
            assertEquals(
                expectedSizes[i],
                partitions.get(i).count(),
                "Partition had wrong size #" + i + ": " + partitions.get(i)
            );
        }
    }
}
