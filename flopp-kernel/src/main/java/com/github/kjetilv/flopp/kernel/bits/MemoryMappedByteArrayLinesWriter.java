package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.io.LinesWriter;

import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Objects;

class MemoryMappedByteArrayLinesWriter implements LinesWriter<String> {

    private final RandomAccessFile randomAccessFile;

    private final Charset charset;

    private final BytesDumpingRingBuffer bytesDumpingRingBuffer;

    MemoryMappedByteArrayLinesWriter(Path target, int bufferSize, Charset charset) {
        try {
            this.randomAccessFile = new RandomAccessFile(target.toFile(), "rw");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open " + target, e);
        }
        this.charset = Objects.requireNonNull(charset, "charset");
        byte[] bytes = new byte[bufferSize];
        this.bytesDumpingRingBuffer = new BytesDumpingRingBuffer(
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
        bytesDumpingRingBuffer.accept(line.getBytes(charset), (byte) '\n');
    }

    @Override
    public void close() {
        bytesDumpingRingBuffer.close();
        try {
            randomAccessFile.close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close " + randomAccessFile, e);
        }
    }
}
