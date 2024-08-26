package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.io.LinesWriter;

import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.file.Path;

class MemoryMappedByteArrayLinesWriter implements LinesWriter {

    private final RandomAccessFile randomAccessFile;

    private final Charset charset;

    private final DumpingRingBuffer dumpingRingBuffer;

    MemoryMappedByteArrayLinesWriter(Path target, int bufferSize, Charset charset) {
        try {
            this.randomAccessFile = new RandomAccessFile(target.toFile(), "rw");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open " + target, e);
        }
        this.charset = charset;
        byte[] bytes = new byte[bufferSize];
        this.dumpingRingBuffer = new DumpingRingBuffer(
            bytes,
            length -> {
                try {
                    randomAccessFile.write(bytes, 0, length);
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to write " + length + " bytes to " + randomAccessFile, e);
                }
            }
        );
    }

    @Override
    public void accept(String line) {
        dumpingRingBuffer.accept(line.getBytes(charset), (byte) '\n');
    }

    @Override
    public void close() {
        dumpingRingBuffer.close();
        try {
            randomAccessFile.close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close " + randomAccessFile, e);
        }
    }
}
