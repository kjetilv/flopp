package com.github.kjetilv.lopp.lw;

import com.github.kjetilv.lopp.FileShape;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Path;

class MemoryMappedByteBufferLinesWriter implements LinesWriter {

    private final RandomAccessFile randomAccessFile;

    private final ByteBuffer byteBuffer;

    private final Charset charset;

    MemoryMappedByteBufferLinesWriter(FileShape fileShape, Path target) {
        this(target, 0, fileShape.charset());
    }

    MemoryMappedByteBufferLinesWriter(Path target, int bufferSize, Charset charset) {
        try {
            this.randomAccessFile = new RandomAccessFile(target.toFile(), "rw");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open " + target, e);
        }
        this.byteBuffer = ByteBuffer.allocate(bufferSize > 0 ? bufferSize : DEFAULT_BUFFER_SIZE);
        this.charset = charset;
    }

    @Override
    public void accept(String line) {
        byte[] bytes = line.getBytes(charset);
        try {
            int left = bytes.length;
            int written = 0;
            while (left > 0) {
                int headroom = byteBuffer.capacity() - byteBuffer.position();
                int delta = Math.min(left, headroom);
                byteBuffer.put(bytes, written, delta);
                left -= delta;
                written += delta;
                if (byteBuffer.position() == byteBuffer.capacity()) {
                    try {
                        flush(byteBuffer, randomAccessFile);
                    } finally {
                        byteBuffer.clear();
                    }
                }
            }
        } finally {
            byteBuffer.put((byte) 10);
        }
    }

    @Override
    public void close() {
        try {
            randomAccessFile.close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close " + randomAccessFile, e);
        }
    }

    public static final int DEFAULT_BUFFER_SIZE = 8192;

    private static void flush(ByteBuffer byteBuffer, RandomAccessFile randomAccessFile) {
        try {
            int length = byteBuffer.position();
            randomAccessFile.write(byteBuffer.array(), 0, length);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to write " + byteBuffer + " to " + randomAccessFile, e);
        }
    }
}
