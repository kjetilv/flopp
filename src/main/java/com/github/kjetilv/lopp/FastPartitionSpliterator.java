package com.github.kjetilv.lopp;

import com.github.kjetilv.lopp.utils.Non;
import com.github.kjetilv.lopp.utils.Pool;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

final class FastPartitionSpliterator extends Spliterators.AbstractSpliterator<NPLine> {

    private final Partition partition;

    private final MappedByteBuffer mappedByteBuffer;

    private final Pool<byte[]> buffers;

    private final Charset charset;

    private int traversed;

    private long nextLineNo;

    private int shipped;

    private boolean firstLineFound;

    private boolean trailing;

    private Slice currentSlice;

    private byte[] currentLineBytes;

    private int currentLinePointer;

    FastPartitionSpliterator(FileChannel fileChannel, Partition partition, FileShape fileShape, int sliceSize) {
        super(Long.MAX_VALUE, ORDERED | IMMUTABLE);
        if (fileShape.stats() == null) {
            throw new IllegalArgumentException("Expected stats for file: " + fileShape);
        }
        this.partition = Objects.requireNonNull(partition, "partitionBytes");
        this.buffers = Pool.byteArrays(sliceSize);
        int longestLine = fileShape.stats().longestLine() > 0
            ? fileShape.stats().longestLine()
            : DEFAULT_LONGEST_LINE;
        long traverseLimit = Math.min(
            partition.count() + (partition.last() ? 0 : longestLine),
            fileShape.stats().fileSize() - partition.offset()
        );
        try {
            mappedByteBuffer = fileChannel.map(
                FileChannel.MapMode.READ_ONLY,
                partition.offset(),
                traverseLimit
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open " + fileChannel, e);
        }
        this.currentSlice = Slice.first(
            Non.negativeOrZero(sliceSize, "sliceSize"),
            traverseLimit
        );
        this.currentLineBytes = new byte[longestLine];
        this.charset = fileShape.charset();

        this.firstLineFound = partition.first();
    }

    @Override
    public boolean tryAdvance(Consumer<? super NPLine> action) {
        byte[] bytes = buffers.acquire();
        try {
            int length = currentSlice.length();
            int bytesToRead = Math.min(length, mappedByteBuffer.limit() - currentSlice.offset());
            mappedByteBuffer.get(currentSlice.offset(), bytes, 0, bytesToRead);
            while (!firstLineFound) {
                if (traversed == bytes.length) {
                    return true;
                }
                byte b = bytes[traversed++];
                if (b == '\n') {
                    firstLineFound = true;
                }
                if (traversed == bytesToRead) {
                    return false;
                }
            }
            boolean firstCall = traversed == 0;
            int firstCallOffset = firstCall ? 0 : traversed;
            for (int i = firstCallOffset; i < bytesToRead; i++) {
                if (trailing && traversed - currentLinePointer > partition.count()) {
                    return false;
                }
                try {
                    byte b = bytes[traversed];
                    if (b == '\n') {
                        String line = new String(currentLineBytes, 0, currentLinePointer, charset);
                        NPLine npLine = new NPLine(line, nextLineNo, partition.partitionNo());
                        action.accept(npLine);
                        shipped++;
                        nextLineNo++;
                        currentLinePointer = 0;
                        if (trailing) {
                            return false;
                        }
                    } else {
                        if (currentLinePointer == currentLineBytes.length) {
                            currentLineBytes = grow(currentLineBytes);
                        }
                        this.currentLineBytes[currentLinePointer++] = b;
                    }
                } finally {
                    traversed++;
                }
                if (traversed > partition.count()) {
                    if (partition.last()) {
                        return false;
                    }
                    trailing = true;
                }
            }
            if (partition.last() && traversed == partition.count()) {
                return false;
            }
            currentSlice = currentSlice.next();
            return true;
        } catch (Exception e) {
            throw new IllegalStateException(this + ": Failed to advance in partition", e);
        } finally {
            buffers.release(bytes);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + ": " + shipped + " lines]";
    }

    public static final int DEFAULT_LONGEST_LINE = 1024;

    private static byte[] grow(byte[] array) {
        byte[] newCurrentLinebytes = new byte[array.length * 2];
        System.arraycopy(array, 0, newCurrentLinebytes, 0, array.length);
        return newCurrentLinebytes;
    }
}
