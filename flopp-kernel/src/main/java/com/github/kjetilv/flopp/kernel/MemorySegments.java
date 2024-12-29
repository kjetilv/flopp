package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.util.Bits;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

public final class MemorySegments {

    public static MemorySegment ofLength(long length, boolean direct) {
        int capacity = alignedSize(length);
        return of(ALLOW_INDIRECT && !direct
            ? ByteBuffer.allocate(capacity)
            : ByteBuffer.allocateDirect(capacity));
    }

    public static MemorySegment of(ByteBuffer byteBuffer) {
        return MemorySegment.ofBuffer(byteBuffer);
    }

    public static MemorySegment ofLength(long length) {
        return ofLength(length, false);
    }

    private static final boolean ALLOW_INDIRECT = false;

    public static MemorySegment of(String string, Charset charset) {
        return of(string, charset, false);
    }

    public static MemorySegment of(String string, Charset charset, boolean direct) {
        return of(string.getBytes(charset), direct);
    }

    public static MemorySegment of(byte[] bytes) {
        return of(bytes, false);
    }

    public static MemorySegment of(byte[] bytes, int offset, int length) {
        return of(bytes, offset, length, false);
    }

    public static MemorySegment of(byte[] bytes, boolean direct) {
        return MemorySegment.ofBuffer(alignedByteBuffer(bytes, direct));
    }

    public static MemorySegment of(byte[] bytes, int offset, int length, boolean direct) {
        return MemorySegment.ofBuffer(alignedByteBuffer(bytes, offset, length, direct));
    }

    public static long bytesAt(MemorySegment memorySegment, long offset, long count) {
        long data = 0;
        long lastPosition = offset + count - 1;
        for (long i = 0; i < count; i++) {
            byte b = memorySegment.get(JAVA_BYTE, lastPosition - i);
            data = (data << ALIGNMENT) + b;
        }
        return data;
    }

    public static long tail(MemorySegment ms, long end) {
        long tailLen = end % ALIGNMENT_INT;
        if (tailLen == 0L) {
            return 0L;
        }
        long value = ms.get(JAVA_LONG, end - tailLen);
        long shift = ALIGNMENT_INT * (ALIGNMENT_INT - tailLen);
        return value << shift >>> shift;
    }

    public static MemorySegment createAligned(long length) {
        return MemorySegment.ofBuffer(
            ByteBuffer.allocateDirect(
                Math.toIntExact(
                    MemorySegments.alignedSize(length))));
    }

    public static MemorySegment alignmentPadded(MemorySegment source, long offset) {
        long size = source.byteSize() - offset;
        long tail = size % ALIGNMENT_INT;
        if (tail == 0) {
            return source;
        }
        int alignedSize = alignedSize(size);
        ByteBuffer resizedBuffer = ByteBuffer.allocateDirect(alignedSize);
        MemorySegment resizedCopy = MemorySegment.ofBuffer(resizedBuffer);
        copyBytes(source, offset, resizedCopy, size);
        return resizedCopy;
    }

    public static String fromEdgeLong(
        MemorySegment memorySegment,
        long startIndex,
        long endIndex,
        byte[] target,
        Charset charset
    ) {
        int tailLength = (int) endIndex % ALIGNMENT_INT;
        if (tailLength == 0) {
            return fromLongsWithinBounds(memorySegment, startIndex, endIndex, target, charset);
        }
        long underlyingSize = memorySegment.byteSize();
        int length = (int) (endIndex - startIndex);
        if (underlyingSize < ALIGNMENT_INT) {
            byte[] bytes = target == null ? new byte[length] : target;
            long data = bytesAt(memorySegment, 0, length);
            Bits.transferLimitedDataTo(data, 0, length, bytes);
            return new String(bytes, 0, length, charset);
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
        long length = Math.max(0, endIndex - startIndex);

        long headOffset = startIndex % ALIGNMENT_INT;
        long tailLength = endIndex % ALIGNMENT_INT;

        long alignedStart = startIndex - headOffset;
        long alignedEnd = tailLength == 0
            ? endIndex
            : endIndex + ALIGNMENT - tailLength;

        long size = alignedEnd - alignedStart;

        byte[] bytes = target == null ? new byte[(int) size] : target;
        for (int index = 0; index < size; index += ALIGNMENT_INT) {
            long data = segment.get(JAVA_LONG, alignedStart + index);
            Bits.transferDataTo(data, index, bytes);
        }
        return new String(bytes, (int) headOffset, (int) length, charset);
    }

    public static void copyBytes(
        MemorySegment from,
        long srcOffset,
        MemorySegment to,
        long length
    ) {
        copyBytes(from, srcOffset, to, 0, length);
    }

    public static void copyBytes(
        MemorySegment from,
        MemorySegment to,
        long dstOffset,
        long length
    ) {
        copyBytes(from, 0, to, dstOffset, length);
    }

    public static void copyBytes(
        MemorySegment from,
        long srcOffset,
        MemorySegment to,
        long dstOffset,
        long length
    ) {
        MemorySegment.copy(
            from,
            JAVA_BYTE,
            srcOffset,
            to,
            JAVA_BYTE,
            dstOffset,
            length
        );
    }

    public static int alignedSize(long size) {
        return (int) (size + ALIGNMENT_INT - size % ALIGNMENT);
    }

    private MemorySegments() {
    }

    public static final long ALIGNMENT = JAVA_LONG.byteAlignment();

    public static final int ALIGNMENT_INT = Math.toIntExact(ALIGNMENT);

    public static final int ALIGNMENT_POW = 3;

    private static ByteBuffer alignedByteBuffer(byte[] bytes, boolean direct) {
        return alignedByteBuffer(bytes, bytes.length, 0, direct);
    }

    private static ByteBuffer alignedByteBuffer(byte[] bytes, int length, int offset, boolean direct) {
        int alignedSize = alignedSize(length);
        int max = Math.max(ALIGNMENT_INT, alignedSize);
        ByteBuffer bb = ALLOW_INDIRECT && !direct
            ? ByteBuffer.allocate(max)
            : ByteBuffer.allocateDirect(max);
        bb.put(bytes, offset, length);
        bb.put(new byte[alignedSize - length]);
        bb.flip();
        return bb;
    }
}

