package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static java.lang.foreign.ValueLayout.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class MemorySegments {

    public static MemorySegment of(String string) {
        return of(string, null);
    }

    public static MemorySegment of(ByteBuffer byteBuffer) {
        return MemorySegment.ofBuffer(byteBuffer);
    }

    public static MemorySegment ofLength(int length) {
        return of(ByteBuffer.allocateDirect(alignedSize(length)));
    }

    public static MemorySegment of(String string, Charset charset) {
        return of(string.getBytes(charset == null ? UTF_8 : charset));
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

    public static MemorySegment alignmentPadded(MemorySegment segment) {
        long size = segment.byteSize();
        int tail = Math.toIntExact(size % ALIGNMENT_INT);
        if (tail == 0) {
            return segment;
        }
        ByteBuffer resizedBuffer = ByteBuffer.allocateDirect(alignedSize(size));
        MemorySegment resizedCopy = MemorySegment.ofBuffer(resizedBuffer);
        MemorySegment.copy(
            segment,
            JAVA_BYTE,
            0,
            resizedCopy,
            JAVA_BYTE,
            0,
            size
        );
        return resizedCopy;
    }

    public static String fromEdgeLong(
        MemorySegment memorySegment,
        long startIndex,
        long endIndex,
        byte[] target,
        Charset charset
    ) {
        long underlyingSize = memorySegment.byteSize();
        int length = (int) (endIndex - startIndex);
        if (underlyingSize < ALIGNMENT_INT) {
            byte[] bytes = target == null ? new byte[length] : target;
            long data = bytesAt(memorySegment, 0, length);
            Bits.transferLimitedDataTo(data, 0, length, bytes);
            return new String(bytes, 0, length, charset == null ? UTF_8 : charset);
        }
        int tailLength = (int) endIndex % ALIGNMENT_INT;
        if (tailLength == 0) {
            return fromLongsWithinBounds(memorySegment, startIndex, endIndex, target, charset);
        }
        int tailStart = (int) (endIndex - tailLength);
        if (tailStart + ALIGNMENT <= underlyingSize) {
            return fromLongsWithinBounds(memorySegment, startIndex, endIndex, target, charset);
        }

        int headOffset = (int) startIndex % ALIGNMENT_INT;

        long alignedStart = startIndex - headOffset;
        int size = (int) (tailStart - alignedStart);

        byte[] bytes = target == null
            ? new byte[(headOffset > 0 ? 0 : ALIGNMENT_INT) + size + ALIGNMENT_INT]
            : target;

        for (int index = 0; index < size; index += ALIGNMENT_INT) {
            long body = memorySegment.get(JAVA_LONG, alignedStart + index);
            Bits.transferDataTo(body, index, bytes);
        }

        long tailLong = memorySegment.get(JAVA_LONG_UNALIGNED, endIndex - ALIGNMENT);
        long adjustedTail = tailLong >> ALIGNMENT * (ALIGNMENT - tailLength);
        Bits.transferLimitedDataTo(adjustedTail, size, tailLength, bytes);

        return new String(
            bytes,
            headOffset,
            length,
            charset == null ? UTF_8 : charset
        );

    }

    public static String fromLongsWithinBounds(
        MemorySegment segment,
        long startIndex,
        long endIndex,
        byte[] target,
        Charset charset
    ) {
        int length = (int) (endIndex - startIndex);

        int headOffset = (int) startIndex % ALIGNMENT_INT;
        int tailLength = (int) endIndex % ALIGNMENT_INT;

        long alignedStart = startIndex - headOffset;
        long alignedEnd = endIndex + (tailLength > 0 ? ALIGNMENT - tailLength : 0);

        int size = (int) (alignedEnd - alignedStart);

        byte[] bytes = target == null ? new byte[size] : target;
        for (int index = 0; index < size; index += ALIGNMENT_INT) {
            long data = segment.get(JAVA_LONG, alignedStart + index);
            Bits.transferDataTo(data, index, bytes);
        }
        return new String(bytes, headOffset, length, charset == null ? UTF_8 : charset);
    }

    private MemorySegments() {
    }

    public static final long ALIGNMENT = JAVA_LONG.byteAlignment();

    public static final int ALIGNMENT_INT = Math.toIntExact(ALIGNMENT);

    private static int alignedSize(long size) {
        return Math.toIntExact(size + ALIGNMENT_INT - size % ALIGNMENT);
    }

    private static ByteBuffer byteBuffer(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.allocateDirect(Math.max(ALIGNMENT_INT, bytes.length));
        bb.put(bytes);
        bb.flip();
        return bb;
    }
}

