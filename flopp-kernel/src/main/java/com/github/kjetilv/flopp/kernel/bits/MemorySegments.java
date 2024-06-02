package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static java.lang.foreign.ValueLayout.*;

public final class MemorySegments {

    public static MemorySegment of(ByteBuffer byteBuffer) {
        return MemorySegment.ofBuffer(byteBuffer);
    }

    public static MemorySegment ofLength(int length) {
        return of(ByteBuffer.allocateDirect(alignedSize(length)));
    }

    public static MemorySegment of(String string, Charset charset) {
        return of(string.getBytes(charset));
    }

    public static MemorySegment of(byte[] bytes) {
        return MemorySegment.ofBuffer(byteBuffer(bytes));
    }

    public static long bytesAt(MemorySegment memorySegment, long offset, long count) {
        long bytes = 0;
        for (long i = count - 1; i >= 0; i--) {
            byte b = memorySegment.get(JAVA_BYTE, offset + i);
            bytes = (bytes << ALIGNMENT) + (b & 0xFFL);
        }
        return bytes;
    }

    public static long tail(MemorySegment ms, long end) {
        int tail = (int) (end % LineSegment.ALIGNMENT_INT);
        long value = ms.get(JAVA_LONG, end - tail);
        int shift = LineSegment.ALIGNMENT_INT * (LineSegment.ALIGNMENT_INT - tail);
        return value << shift >> shift;
    }

    public static MemorySegment alignmentPadded(MemorySegment segment, long offset) {
        long size = segment.byteSize() - offset;
        int tail = (int)(size % ALIGNMENT_INT);
        if (tail == 0) {
            return segment;
        }
        ByteBuffer resizedBuffer = ByteBuffer.allocateDirect(alignedSize(size));
        MemorySegment resizedCopy = MemorySegment.ofBuffer(resizedBuffer);
        MemorySegment.copy(
            segment,
            JAVA_BYTE,
            offset,
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
            return new String(bytes, 0, length, charset);
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

        long tailLong = tail(memorySegment, endIndex);
        long adjustedTail = tailLong >> ALIGNMENT * (ALIGNMENT - tailLength);
        Bits.transferLimitedDataTo(adjustedTail, size, tailLength, bytes);
        return new String(bytes, headOffset, length, charset);
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
        return new String(bytes, headOffset, length, charset);
    }

    private MemorySegments() {
    }

    public static final long ALIGNMENT = JAVA_LONG.byteAlignment();

    public static final int ALIGNMENT_INT = (int) ALIGNMENT;

    private static int alignedSize(long size) {
        return (int)(size + ALIGNMENT_INT - size % ALIGNMENT);
    }

    private static ByteBuffer byteBuffer(byte[] bytes) {
        int length = bytes.length;
        int alignedSize = alignedSize(length);
        ByteBuffer bb = ByteBuffer.allocateDirect(Math.max(ALIGNMENT_INT, alignedSize));
        bb.put(bytes);
        bb.put(new byte[alignedSize - length]);
        bb.flip();
        return bb;
    }
}

