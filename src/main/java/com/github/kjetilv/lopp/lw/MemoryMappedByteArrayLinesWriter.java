package com.github.kjetilv.lopp.lw;

import com.github.kjetilv.lopp.utils.DumpingRingBuffer;
import com.github.kjetilv.lopp.utils.Pool;

import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.file.Path;

public class MemoryMappedByteArrayLinesWriter implements LinesWriter {

    private final RandomAccessFile randomAccessFile;

    private final Charset charset;

    private final Pool<byte[]> byteArrays;

    private final DumpingRingBuffer dumpingRingBuffer;

    MemoryMappedByteArrayLinesWriter(Path target, Charset charset) {
        this(target, 0, charset);
    }

    public MemoryMappedByteArrayLinesWriter(Path target, int bufferSize, Charset charset) {
        try {
            this.randomAccessFile = new RandomAccessFile(target.toFile(), "rw");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open " + target, e);
        }
        this.byteArrays = Pool.byteArrays(bufferSize > 0 ? bufferSize : DEFAULT_BUFFER_SIZE);
        this.charset = charset;
        this.dumpingRingBuffer = new DumpingRingBuffer(
            byteArrays::acquire,
            (bytes, length) -> {
                try {
                    try {
                        randomAccessFile.write(bytes, 0, length);
                    } catch (Exception e) {
                        throw new IllegalStateException(
                            "Failed to write " + length + " bytes to " + randomAccessFile, e);
                    }
                } finally {
                    byteArrays.release(bytes);
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

    private static final int DEFAULT_BUFFER_SIZE = 8192;

}
