package com.github.kjetilv.flopp.kernel;

import java.util.function.Consumer;

abstract class AbstractLimitedPartitionSpliterator<T> extends AbstractPartitionSpliterator<T> {
    /**
     * The source of our bytes
     */
    AbstractLimitedPartitionSpliterator(
        int bufferSize,
        ByteSource byteSource,
        Partition partition,
        Shape shape
    ) {
        super(bufferSize, byteSource, partition, shape);
    }

    @SuppressWarnings("DuplicatedCode")
    protected boolean process(Consumer<? super T> action) {
        while (true) {
            long bytesToRead = byteSource.fill(byteBuffer);
            int bufferIndex = 0;
            if (!firstLineFound) { // Still haven't found first line, still on the previous partition's trail
                for (; bufferIndex < bytesToRead; bufferIndex++) { // Fast forward ...
                    partitionIndex++; // Count up number of bytes processed
                    // Ok, so next byte is ...
                    if (byteBuffer[bufferIndex] == '\n') { // Found it!
                        firstLineFound = true;
                        bufferIndex++;
                        break;
                    }
                }
                if (!firstLineFound || bufferIndex == bytesToRead) { // Still no line!
                    if (sliceDone()) {
                        return done();
                    }
                    nextSlice();
                    continue;
                }
            }
            if (!trailing) {
                for (; bufferIndex < bytesToRead; bufferIndex++) { // Found first line, now onwards!
                    // So what's the next byte then?
                    if (byteBuffer[bufferIndex] == '\n') { // We've got a line!
                        shipAndReset(action);
                    } else { // No line yet
                        handleChar(byteBuffer[bufferIndex]);
                    }
                    partitionIndex++; // Whatever we did, count up number of bytes processed
                    if (partitionIndex > partitionCount) { // We are past our byte mark!
                        if (lastPartition) { // This is the last partition
                            return done();
                        }
                        trailing = true; // Make a note that we are now in the trailing part of the partition
                        bufferIndex++;
                        break;
                    }
                }
            }
            if (trailing) {
                for (; bufferIndex < bytesToRead; bufferIndex++) { // Found first line, now onwards!
                    byte c = byteBuffer[bufferIndex]; // So what's the next byyte then?
                    if (c == '\n') { // We've got a line!
                        shipAndReset(action);
                        return done();
                    } else { // No line yet
                        handleChar(c);
                    }
                    partitionIndex++; // Whatever we did, count up number of bytes processed
                    if (partitionIndex > partitionCount + lineIndex) { // What does this mean?
                        return done();
                    }
                }
            }
            if (lastPartition && partitionIndex == partitionCount) { // We've exhausted the last partition
                return done();
            }
            nextSlice();
        }
    }

    private void handleChar(byte c) {
        lineBytes[lineIndex] = c; // Remember the byte for the upcoming line
        lineIndex++; // Count up our position on the current line
    }
}
