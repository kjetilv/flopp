package com.github.kjetilv.lopp.lc;

import com.github.kjetilv.lopp.FileShape;
import com.github.kjetilv.lopp.Partition;
import com.github.kjetilv.lopp.PartitionBytes;
import com.github.kjetilv.lopp.Partitioning;
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

        Partitioning partitioning = new Partitioning(partitionCount, 16, res);
        FileShape fileShape = FileShape.base().fileSize(size);

        ByteIndexEstimator tracker = new ByteIndexEstimator(fileShape, partitioning);
        for (int i = 0; i < size; i += lineBytes) {
            tracker.lineAt(i);
        }

        List<PartitionBytes> partitionBytes = tracker.bytesPartitions();
        partitionBytes.forEach(System.out::println);

        Partition.partitions(lineCount, FileShape.base().header(1, 1), partitioning.partitionCount())
            .forEach(System.out::println);
    }
}
