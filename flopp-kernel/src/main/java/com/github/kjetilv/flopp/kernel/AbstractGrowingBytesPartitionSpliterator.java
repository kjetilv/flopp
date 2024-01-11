package com.github.kjetilv.flopp.kernel;

import java.util.function.Consumer;

abstract class AbstractGrowingBytesPartitionSpliterator<T> extends AbstractBytesPartitionSpliterator<T> {

    AbstractGrowingBytesPartitionSpliterator(int bufferSize, ByteSource byteSource, Partition partition, Shape shape) {
        super(bufferSize, byteSource, partition, shape);
    }

    @Override
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
                        handleChar(byteBuffer[bufferIndex]);
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
                    byte c = byteBuffer[bufferIndex];
                    if (c == '\n') {
                        shipAndReset(action);
                        return done();
                    } else {
                        handleChar(c);
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
            if (sliceDone()) {
                expandBuffer();
                limit = computeLength();
            }
            nextSlice();
        }
    }

    private final void handleChar(byte c) {
        try {
            lineBytes[lineIndex] = c;
        } catch (ArrayIndexOutOfBoundsException e) {
            if (lineIndex == maxLineLength) {
                expandBuffer();
                lineBytes[lineIndex] = c;
            } else {
                throw new RuntimeException(e);
            }
        }
        lineIndex++;
    }

    private void expandBuffer() {
        maxLineLength *= 2;
        byte[] newCurrentLinebytes = new byte[maxLineLength];
        System.arraycopy(lineBytes, 0, newCurrentLinebytes, 0, lineIndex);
        lineBytes = newCurrentLinebytes;
    }
}
