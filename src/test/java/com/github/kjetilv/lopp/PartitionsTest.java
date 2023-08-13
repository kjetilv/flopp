package com.github.kjetilv.lopp;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionsTest {

    @Test
    public void test1418() {
        List<Partition> partitions = Partition.partitions(18, TWO_BY_TWO, 4);
        assertSizes(partitions, 4L, 4L, 3L, 3L);
    }

    @Test
    public void test12zero() {
        List<Partition> partitions = Partition.partitions(12,  4);
        assertSizes(partitions, 3L, 3L, 3L, 3L);
    }

    @Test
    public void test12one() {
        List<Partition> partitions = Partition.partitions(
            13,
            new FileShape(new FileShape.Decor(1, 0)),
            4);
        assertSizes(partitions, 3L, 3L, 3L, 3L);
    }

    @Test
    public void test1216() {
        List<Partition> partitions = Partition.partitions(16, TWO_BY_TWO, 3);
        assertSizes(partitions, 4L, 4L, 4L);
    }

    @Test
    public void testALot() {
        List<Partition> partitions = Partition.partitions(100_004, TWO_BY_TWO, 4);
        assertSizes(partitions, 25_000L, 25_000L, 25_000L, 25_000L);
    }

    @Test
    public void testALotPlus1() {
        List<Partition> partitions = Partition.partitions(100_005, TWO_BY_TWO, 4);
        assertSizes(partitions, 25_001L, 25_000L, 25_000L, 25_000L);
    }

    @Test
    public void testALotMin1() {
        List<Partition> partitions = Partition.partitions(100_003, TWO_BY_TWO, 4);
        assertSizes(partitions, 25_000L, 25_000L, 25_000L, 24_999L);
    }

    public static final FileShape TWO_BY_TWO =
        new FileShape(new FileShape.Decor(2, 2));

    private static void assertSizes(List<Partition> partitions, Long... expectedSizes) {
        assertEquals(partitions.size(), expectedSizes.length);
        for (int i = 0; i < partitions.size(); i++) {
            assertEquals(
                expectedSizes[i],
                partitions.get(i).count(),
                "Partition had wrong size: " + partitions.get(i));
        }
    }
}
