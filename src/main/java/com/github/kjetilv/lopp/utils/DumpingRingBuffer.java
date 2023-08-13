package com.github.kjetilv.lopp.utils;

import java.io.Closeable;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class DumpingRingBuffer implements Closeable {

    private final Supplier<byte[]> buffers;

    private final BiConsumer<byte[], Integer> dump;

    private byte[] buffer;

    private int bufferIndex;

    private int bufferLeft;

    private int bufferLength;

    public DumpingRingBuffer(Supplier<byte[]> buffer, BiConsumer<byte[], Integer> dump) {
        this.buffers = Objects.requireNonNull(buffer, "buffer");
        this.dump = Objects.requireNonNull(dump, "dump");

        newBuffer();
    }

    public void accept(byte[] bytes, byte additional) {
        int bytesLeft = bytes.length + 1;
        if (bytesLeft <= bufferLeft) {
            writeRemaining(bytes, additional);
        } else {
            int written = 0;
            while (bytesLeft > bufferLeft) {
                int bytesWritten = padToEnd(bytes, written);
                written += bytesWritten;
                bytesLeft -= bytesWritten;
                dump();
                newBuffer();
            }
            writeRemaining(bytes, additional, written);
        }
    }

    @Override
    public void close() {
        dump();
    }

    private void dump() {
        dump.accept(buffer, bufferIndex);
    }

    private int padToEnd(byte[] bytes, int offset) {
        int written = bufferLeft;
        System.arraycopy(bytes, offset, buffer, bufferIndex, bufferLeft);
        bufferIndex = bufferLength;
        bufferLeft = 0;
        return written;
    }

    private void writeRemaining(byte[] bytes, byte additional) {
        writeRemaining(bytes, additional, 0);
    }

    private void writeRemaining(byte[] bytes, byte additional, int offset) {
        int size = bytes.length - offset;
        System.arraycopy(bytes, offset, buffer, bufferIndex, size);
        bufferIndex += size;
        bufferLeft -= size;
        buffer[bufferIndex] = additional;
        bufferIndex++;
        bufferLeft--;
    }

    private void newBuffer() {
        this.bufferIndex = 0;
        this.buffer = buffers.get();
        this.bufferLength = this.buffer.length;
        this.bufferLeft = bufferLength;
    }
}
