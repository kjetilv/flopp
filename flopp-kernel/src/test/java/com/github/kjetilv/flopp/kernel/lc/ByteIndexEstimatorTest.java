package com.github.kjetilv.flopp.kernel.lc;

import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Shape;
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

        Partitioning.count(partitioning.partitionCount()).of(lineCount)
            .forEach(System.out::println);
    }
}
