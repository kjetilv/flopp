package com.github.kjetilv.flopp.lc;

import com.github.kjetilv.flopp.Partition;
import com.github.kjetilv.flopp.Partitioning;
import com.github.kjetilv.flopp.Shape;
import org.junit.jupiter.api.Test;

import java.util.List;

class ByteIndexEstimatorTest {

    @Test
    void test() {
        int partitionCount = 3;

        int lineLength = 10;
        int lineBytes = lineLength + 1;

        int lineCount = 20;
        int size = lineBytes * lineCount;
        int res = 10;

        Partitioning partitioning = new Partitioning(partitionCount, 16);
        Shape shape = Shape.size(size);

        ByteIndexEstimator tracker = new ByteIndexEstimator(shape, partitioning, res);
        for (int i = 0; i < size; i += lineBytes) {
            tracker.lineAt(i);
        }

        List<PartitionBytes> partitionBytes = tracker.bytesPartitions();
        partitionBytes.forEach(System.out::println);

        Partition.partitions(lineCount, partitioning.partitionCount())
            .forEach(System.out::println);
    }
}
