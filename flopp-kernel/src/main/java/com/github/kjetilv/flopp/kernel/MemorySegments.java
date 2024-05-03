package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static java.lang.foreign.ValueLayout.*;

public final class MemorySegments {

    public static MemorySegment of(String string) {
        return of(string, null);
    }

    public static MemorySegment of(ByteBuffer byteBuffer) {
        return MemorySegment.ofBuffer(byteBuffer);
    }

    public static MemorySegment of(String string, Charset charset) {
        return of(string.getBytes(charset == null ? StandardCharsets.UTF_8 : charset));
    }

    public static MemorySegment of(byte[] bytes) {
        return MemorySegment.ofBuffer(byteBuffer(bytes));
    }

    public static long bytesAt(MemorySegment memorySegment, long offset, long count) {
        if (count < ALIGNMENT) {
            return readHead(memorySegment, offset, count);
        }
        long bytes = 0;
        for (long i = count - 1; i >= 0; i--) {
            byte b = memorySegment.get(JAVA_BYTE, offset + i);
            bytes = (bytes << ALIGNMENT) + (b & 0xFFL);
        }
        return bytes;
    }

    public static long readHead(MemorySegment memorySegment, long offset, long length) {
        return switch (Math.toIntExact(length)) {
            case 0 -> 0L;
            case 1 -> memorySegment.get(JAVA_BYTE, offset);
            case 2 -> memorySegment.get(JAVA_SHORT_UNALIGNED, offset);
            case 3 -> (long) memorySegment.get(JAVA_SHORT_UNALIGNED, offset) +
                      ((long) memorySegment.get(JAVA_BYTE, offset + 2) << 16L);
            case 4 -> (long) memorySegment.get(JAVA_INT_UNALIGNED, offset);
            case 5 -> (long) memorySegment.get(JAVA_INT_UNALIGNED, offset) +
                      ((long) memorySegment.get(JAVA_BYTE, offset + 4) << 32L);
            case 6 -> (long) memorySegment.get(JAVA_INT_UNALIGNED, offset) +
                      ((long) memorySegment.get(JAVA_SHORT_UNALIGNED, offset + 4) << 32L);
            case 7 -> (long) memorySegment.get(JAVA_INT_UNALIGNED, offset) +
                      ((long) memorySegment.get(JAVA_SHORT_UNALIGNED, offset + 4) << 32L) +
                      ((long) memorySegment.get(JAVA_BYTE, offset + 6) << 48L);
            default -> throw new IllegalStateException("Invalid head: " + length);
        };
    }

    public static long readTail(MemorySegment memorySegment, long limit, int length) {
        return switch (length) {
            case 0 -> 0L;
            case 1 -> memorySegment.get(JAVA_BYTE, limit - 1);
            case 2 -> memorySegment.get(JAVA_SHORT_UNALIGNED, limit - 2);
            case 3 -> ((long) memorySegment.get(JAVA_SHORT_UNALIGNED, limit - 2) << 8L) +
                      (memorySegment.get(JAVA_BYTE, limit - 3) & 0xFF);
            case 4 -> memorySegment.get(JAVA_INT_UNALIGNED, limit - 4);
            case 5 -> ((long) memorySegment.get(JAVA_INT_UNALIGNED, limit - 4) << 8L) +
                      (memorySegment.get(JAVA_BYTE, limit - 5) & 0xFF);
            case 6 -> ((long) memorySegment.get(JAVA_INT_UNALIGNED, limit - 4) << 16L) +
                      (memorySegment.get(JAVA_SHORT_UNALIGNED, limit - 6) & 0xFFFF);
            case 7 -> ((long) memorySegment.get(JAVA_INT_UNALIGNED, limit - 4) << 24L) +
                      (memorySegment.get(JAVA_SHORT_UNALIGNED, limit - 6) << 8 & 0xFFFFFF) +
                      (memorySegment.get(JAVA_BYTE, limit - 7) & 0xFF);
            default -> throw new IllegalStateException("Invalid tail: " + length);
        };
    }

    private MemorySegments() {
    }

    private static final long ALIGNMENT = 8L;

    private static ByteBuffer byteBuffer(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.allocateDirect(bytes.length);
        bb.put(bytes);
        bb.flip();
        return bb;
    }
}
