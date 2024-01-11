package com.github.kjetilv.flopp.kernel;

import java.util.function.Consumer;

abstract class AbstractLimitedBytesPartitionSpliterator<T> extends AbstractBytesPartitionSpliterator<T> {
    /**
     * The source of our bytes
     */
    AbstractLimitedBytesPartitionSpliterator(int bufferSize, ByteSource byteSource, Partition partition, Shape shape) {
        super(bufferSize, byteSource, partition, shape);
    }

    @SuppressWarnings("DuplicatedCode")
    protected boolean process(Consumer<? super T> action) {
        while (true) {
            long bytesToRead = byteSource.fill(byteBuffer);
            int bufferIndex = 0;
            if (!firstLineFound) {
                for (; bufferIndex < bytesToRead; bufferIndex++) {
                    partitionIndex++;
                    if (byteBuffer[bufferIndex] == '\n') {
                        firstLineFound = true;
                        bufferIndex++;
                        break;
                    }
                }
                if (!firstLineFound || bufferIndex == bytesToRead) {
                    if (sliceDone()) {
                        return done();
                    }
                    nextSlice();
                    continue;
                }
            }
            if (!trailing) {
                for (; bufferIndex < bytesToRead; bufferIndex++) {
                    if (byteBuffer[bufferIndex] == '\n') {
                        shipAndReset(action);
                    } else {
                        lineBytes[lineIndex] = byteBuffer[bufferIndex];
                        lineIndex++;
                    }
                    partitionIndex++;
                    if (partitionIndex > partitionCount) {
                        if (lastPartition) {
                            return done();
                        }
                        trailing = true;
                        bufferIndex++;
                        break;
                    }
                }
            }
            if (trailing) {
                for (; bufferIndex < bytesToRead; bufferIndex++) {
                    if (byteBuffer[bufferIndex] == '\n') {
                        shipAndReset(action);
                        return done();
                    } else {
                        lineBytes[lineIndex] = byteBuffer[bufferIndex];
                        lineIndex++;
                    }
                    partitionIndex++;
                    if (partitionIndex > partitionCount + lineIndex) {
                        return done();
                    }
                }
            }
            if (lastPartition && partitionIndex == partitionCount) {
                return done();
            }
            nextSlice();
        }
    }

}
