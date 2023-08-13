package com.github.kjetilv.lopp;

import com.github.kjetilv.lopp.utils.Non;
import com.github.kjetilv.lopp.utils.Pool;

import java.nio.MappedByteBuffer;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

final class PartitionSpliterator extends Spliterators.AbstractSpliterator<NPLine> {

    private final PartitionBytes partitionBytes;
    private final MappedByteBuffer mappedByteBuffer;
    private final byte[] lineBuffer;
    private final long expected;
    private final Pool<byte[]> buffers;
    private final int partitionNo;
    private final Charset charset;
    private int bufferPointer;
    private int shipped;
    private long nextLineNo;
    private Slice currentSlice;

    PartitionSpliterator(
        MappedByteBuffer mappedByteBuffer,
        PartitionBytes partitionBytes,
        FileShape fileShape,
        int sliceSize
    ) {
        super(count(partitionBytes), ORDERED | IMMUTABLE);
        this.partitionBytes = Objects.requireNonNull(partitionBytes, "partitionBytes");
        this.buffers = Pool.byteArrays(sliceSize);
        Partition partition = this.partitionBytes.partition();

        this.expected = partition.count();
        this.mappedByteBuffer = Objects.requireNonNull(mappedByteBuffer, "map");
        this.lineBuffer = new byte[fileShape.stats().longestLine()];
        this.nextLineNo = partition.offset();
        this.currentSlice = Slice.first(
            Non.negativeOrZero(sliceSize, "sliceSize"),
            this.partitionBytes.count()
        );
        partitionNo = this.partitionBytes.partition().partitionNo();
        charset = fileShape.charset();
    }

    private static long count(PartitionBytes partitionBytes) {
        return Objects.requireNonNull(partitionBytes, "partition").partition().count();
    }

    @Override
    public boolean tryAdvance(Consumer<? super NPLine> action) {
        if (shipped < expected) {
            byte[] bytes = buffers.acquire();
            try {
                int length = currentSlice.length();
                mappedByteBuffer.get(currentSlice.offset(), bytes, 0, length);
                transfer(action, bytes, length);
                currentSlice = currentSlice.next();
                return true;
            } catch (Exception e) {
                throw new IllegalStateException(this + ": Failed to advance in partition", e);
            } finally {
                buffers.release(bytes);
            }
        }
        return false;
    }

    private void transfer(Consumer<? super NPLine> action, byte[] bytes, int length) {
        int index = 0;
        while (index < length && shipped < expected) {
            try {
                byte b = bytes[index];
                if (b == '\n') {
                    String line = new String(lineBuffer, 0, bufferPointer, charset);
                    NPLine npLine = new NPLine(line, nextLineNo, partitionNo);
                    action.accept(npLine);
                    shipped++;
                    nextLineNo++;
                    bufferPointer = 0;
                } else {
                    this.lineBuffer[bufferPointer++] = b;
                }
            } finally {
                index++;
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partitionBytes + ": " + shipped + "/" + expected + " lines]";
    }
}
