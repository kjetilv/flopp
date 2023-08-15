package com.github.kjetilv.lopp.files;

import java.io.Closeable;
import java.util.Objects;
import java.util.function.IntConsumer;

class DumpingRingBuffer implements Closeable {

    private final byte[] buffer;

    private final IntConsumer dump;

    private int bufferIndex;

    private int bufferLeft;

    private int bufferLength;

    DumpingRingBuffer(byte[] buffer, IntConsumer dump) {
        this.buffer = buffer;
        this.dump = Objects.requireNonNull(dump, "dump");
        newBuffer();
    }

    public void accept(byte[] bytes, byte additional) {
        int bytesLeft = bytes.length + 1;
        if (bytesLeft <= bufferLeft) {
            writeRemaining(bytes, additional, 0);
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
        dump.accept(bufferIndex);
    }

    private int padToEnd(byte[] bytes, int offset) {
        int written = bufferLeft;
        System.arraycopy(bytes, offset, buffer, bufferIndex, bufferLeft);
        bufferIndex = bufferLength;
        bufferLeft = 0;
        return written;
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
        this.bufferLength = this.buffer.length;
        this.bufferLeft = bufferLength;
    }
}
