package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;

import java.lang.foreign.MemorySegment;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Spliterators;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static java.lang.foreign.ValueLayout.*;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;

@SuppressWarnings("DuplicatedCode")
public final class LineSegments {

    public static boolean equals(LineSegment segment1, LineSegment segment2) {
        if (segment1 == null || segment2 == null) {
            return (segment1 == null) == (segment2 == null);
        }
        if (segment1.length() != segment2.length()) {
            return false;
        }
        LongSupplier longSupplier1 = segment1.alignedLongSupplier();
        LongSupplier longSupplier2 = segment2.alignedLongSupplier();
        for (long l = 0; l < segment1.alignedLongsCount(); l++) {
            if (longSupplier1.getAsLong() != longSupplier2.getAsLong()) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("UnnecessaryParentheses")
    public static int hashCode(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0L) {
            return 0;
        }
        long hashCode = 0L;
        int headLen = segment.headLength();
        long alignedStart = segment.alignedStart();
        long alignedEnd = segment.alignedEnd();
        long endIndex = segment.endIndex();
        long tailLen = endIndex % ALIGNMENT;
        long longs = segment.fullLongCount() + (headLen > 0 ? 1 : 0) + (tailLen > 0 ? 1 : 0);
        if (headLen > 0) {
            long data = segment.head(true);
            hashCode = data * 31L ^ (longs == 0 ? 1 : longs);
        }
        if (length > ALIGNMENT) {
            MemorySegment memorySegment = segment.memorySegment();
            long startPosition = alignedStart + (headLen == 0 ? 0 : ALIGNMENT);
            for (long pos = startPosition; pos < alignedEnd; pos += ALIGNMENT) {
                long data = memorySegment.get(JAVA_LONG, pos);
                hashCode += (data * 31L) ^ (longs - (pos / ALIGNMENT));
            }
            if (tailLen > 0) {
                hashCode += readTail(segment, memorySegment, length, endIndex, tailLen, false) * 31L;
            }
        }
        return (int) hashCode;
    }

    @SuppressWarnings("ConstantValue")
    public static int compare(LineSegment segment1, LineSegment segment2) {
        LongSupplier longSupplier1 = segment1.longSupplier(true);
        LongSupplier longSupplier2 = segment2.longSupplier(true);
        long length1 = segment1.shiftedLongsCount();
        long length2 = segment2.shiftedLongsCount();
        long length = Math.min(length1, length2);
        for (int i = 0; i < length; i++) {
            long l1 = longSupplier1.getAsLong();
            long l2 = longSupplier2.getAsLong();
            if (l1 < l2) {
                return -1;
            }
            if (l1 > l2) {
                return 1;
            }
        }
        return length1 < length2 ? -1
            : length2 > length1 ? 1
                : 0;
    }

    public static LongSupplier alignedLongSupplier(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return () -> 0x0L;
        }
        return new LineSegmentAlignedLongSupplier(segment, length);
    }

    public static LongSupplier longSupplier(LineSegment segment, boolean shift) {
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return () -> 0x0L;
        }
        int headLen = segment.headLength();
        if (headLen > 0 && shift) {
            return new LineSegmentShiftedLongSupplier(segment, length, headLen);
        }
        return alignedLongSupplier(segment);
    }

    public static LongStream longs(LineSegment segment, boolean align) {
        return align ? shiftedLongs(segment) : alignedLongs(segment);
    }

    public static LongStream alignedLongs(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return LongStream.empty();
        }
        int headLen = segment.headLength();
        long alignedStart = segment.alignedStart();
        long alignedEnd = segment.alignedEnd();
        return StreamSupport.longStream(
            new Spliterators.AbstractLongSpliterator(
                length / ALIGNMENT + 2,
                IMMUTABLE | ORDERED
            ) {

                @Override
                public boolean tryAdvance(LongConsumer action) {
                    if (headLen > 0) {
                        action.accept(segment.head(true));
                    }
                    if (length > headLen) {
                        long endIndex = segment.endIndex();
                        MemorySegment memorySegment = segment.memorySegment();
                        long startPosition = alignedStart + (headLen == 0 ? 0 : ALIGNMENT);
                        for (long pos = startPosition; pos < alignedEnd; pos += ALIGNMENT) {
                            action.accept(memorySegment.get(JAVA_LONG, pos));
                        }
                        int tailLen = Math.toIntExact(endIndex % ALIGNMENT);
                        if (tailLen > 0) {
                            action.accept(readTail(segment, memorySegment, length, endIndex, tailLen, true));
                        }
                    }
                    return false;
                }
            },
            false
        );
    }

    public static LongStream shiftedLongs(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return LongStream.empty();
        }
        if (length < ALIGNMENT) {
            return LongStream.of(segment.head(true));
        }
        int headLen = segment.headLength();
        if (headLen == 0) {
            return alignedLongs(segment);
        }
        int tailShift = Math.toIntExact((ALIGNMENT - headLen) * ALIGNMENT);
        int headShift = Math.toIntExact(headLen * ALIGNMENT);
        long alignedStart = segment.alignedStart();
        long alignedEnd = segment.alignedEnd();
        long endIndex = segment.endIndex();
        long tailLen = endIndex % ALIGNMENT;
        MemorySegment memorySegment = segment.memorySegment();
        return StreamSupport.longStream(
            new Spliterators.AbstractLongSpliterator(
                length / ALIGNMENT + 2,
                IMMUTABLE | ORDERED
            ) {

                @Override
                public boolean tryAdvance(LongConsumer action) {
                    long data = segment.head(true);
                    long position = alignedStart + ALIGNMENT;
                    while (position < alignedEnd) {
                        try {
                            long alignedData = memorySegment.get(JAVA_LONG, position);
                            data |= alignedData << headShift;
                            action.accept(data);
                            data = alignedData >> tailShift;
                        } finally {
                            position += ALIGNMENT;
                        }
                    }
                    if (tailLen > 0) {
                        long alignedData = readTail(segment, memorySegment, length, endIndex, tailLen, true);
                        data |= alignedData << headShift;
                        action.accept(data);
                    }
                    return false;
                }
            },
            false
        );
    }

    public static String asString(LineSegment segment) {
        long startIndex = segment.startIndex();
        long endIndex = segment.endIndex();
        int length = (int) (endIndex - startIndex);

        MemorySegment memorySegment = segment.memorySegment();
        long underlyingSize = segment.underlyingSize();

        return new String(
            fromLongBytes(
                memorySegment,
                startIndex,
                endIndex,
                length,
                new byte[length],
                underlyingSize
            ),
            StandardCharsets.UTF_8
        );
    }

    public static String asString(LineSegment segment, byte[] buffer) {
        long startIndex = segment.startIndex();
        long endIndex = segment.endIndex();
        int length = (int) (endIndex - startIndex);

        MemorySegment memorySegment = segment.memorySegment();
        long underlyingSize = segment.underlyingSize();

        byte[] bytes = fromLongBytes(
            memorySegment,
            startIndex,
            endIndex,
            length,
            buffer,
            underlyingSize
        );
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }

    public static byte[] simpleBytes(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return NO_BYTES;
        }
        byte[] bytes = new byte[length];
        MemorySegment.copy(
            segment.memorySegment(),
            JAVA_BYTE,
            segment.startIndex(),
            bytes,
            0,
            length
        );
        return bytes;
    }

    public static byte[] fromLongBytes(LineSegment segment, byte[] buffer) {
        long startIndex = segment.startIndex();
        long endIndex = segment.endIndex();
        int length = (int) (endIndex - startIndex);

        MemorySegment memorySegment = segment.memorySegment();
        long underlyingSize = memorySegment.byteSize();

        return fromLongBytes(
            memorySegment,
            startIndex,
            endIndex,
            length,
            buffer,
            underlyingSize
        );
    }

    public static byte[] fromLongBytes(LineSegment segment) {
        long startIndex = segment.startIndex();
        long endIndex = segment.endIndex();
        int length = (int) (endIndex - startIndex);

        MemorySegment memorySegment = segment.memorySegment();
        long underlyingSize = memorySegment.byteSize();

        return fromLongBytes(
            memorySegment,
            startIndex,
            endIndex,
            length,
            new byte[length],
            underlyingSize
        );
    }

    public static byte[] asBytes(LineSegment segment) {
        int length = Math.toIntExact(segment.length());
        if (length == 0) {
            return NO_BYTES;
        }
        byte[] string = new byte[length];
        int headLen = segment.headLength();
        if (headLen > 0) {
            long data = segment.head(false);
            Bits.transferLimitedDataTo(data, 0, Math.min(length, headLen), string);
        }
        if (length > headLen) {
            long alignedStart = segment.alignedStart();
            long longs = (segment.alignedEnd() - alignedStart) / ALIGNMENT;
            int firstLong = headLen == 0 ? 0 : 1;
            long endIndex = segment.endIndex();
            int tailLen = Math.toIntExact(endIndex % ALIGNMENT);
            MemorySegment memorySegment = segment.memorySegment();
            int transferOffset = headLen;
            long position = alignedStart + firstLong * ALIGNMENT;
            for (int i = firstLong; i < longs; i++) {
                long data = memorySegment.get(JAVA_LONG, position);
                Bits.transferDataTo(data, transferOffset, string);
                transferOffset += ALIGNMENT_INT;
                position += ALIGNMENT;
            }
            if (tailLen > 0) {
                long data = readTail(segment, memorySegment, length, endIndex, tailLen, false);
                Bits.transferLimitedDataTo(data, transferOffset, tailLen, string);
            }
        }
        return string;
    }

    public static String asString(LineSegment segment, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = segment.byteAt(i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static LineSegment of(String string) {
        return of(string, null);
    }

    public static LineSegment of(String string, Charset charset) {
        return of(string.getBytes(charset == null ? StandardCharsets.UTF_8 : charset));
    }

    public static LineSegment of(byte[] bytes) {
        return of(MemorySegments.of(bytes));
    }

    public static LineSegment of(MemorySegment memorySegment) {
        return new ImmutableSliceSegment(memorySegment);
    }

    public static LineSegment of(MemorySegment memorySegment, long start, long end) {
        return new ImmutableLineSegment(memorySegment, start, end);
    }

    public static LineSegment of(long l) {
        return of(l, Math.toIntExact(ALIGNMENT));
    }

    public static LineSegment of(long l, int len) {
        return of(new String(Bits.toBytes(l), 0, len));
    }

    public static String toString(LineSegment ls) {
        return ls.getClass().getSimpleName() + "[" + ls.startIndex() + "-" + ls.endIndex() + "]";
    }

    public static String asString(MemorySegment segment, long start, long end) {
        return of(segment, start, end).asString();
    }

    private LineSegments() {
    }

    public static final byte[] NO_BYTES = new byte[0];

    static final long ALIGNMENT = 8L;

    static final int ALIGNMENT_INT = 8;

    private static byte[] fromLongBytes(
        MemorySegment memorySegment,
        long startIndex,
        long endIndex,
        int length,
        byte[] target,
        long underlyingSize
    ) {
        long alignedStart = startIndex - startIndex % ALIGNMENT_INT;
        long alignedEnd = endIndex - endIndex % ALIGNMENT_INT;

        int headOffset = (int) (startIndex - alignedStart);
        int tailLength = (int) (endIndex - alignedEnd);

        int headLength = Math.min(length, headOffset > 0 ? ALIGNMENT_INT - headOffset : 0);

        if (underlyingSize - startIndex < ALIGNMENT_INT) { // Bumping against start
            long head = MemorySegments.readHead(
                memorySegment,
                startIndex,
                headLength
            );
            Bits.transferLimitedDataTo(head, 0, headLength, target);
        } else {
            long head = memorySegment.get(JAVA_LONG_UNALIGNED, startIndex);
            if (length < ALIGNMENT) {
                Bits.transferLimitedDataTo(head, 0, headLength, target);
            } else {
                Bits.transferDataTo(head, 0, target);
            }
        }

        if (length == headLength) {
            return target;
        }

        if (endIndex - ALIGNMENT_INT < 0) { // Bumping against end
            long tail = MemorySegments.readTail(memorySegment, endIndex, tailLength);
            Bits.transferLimitedDataTo(tail, length - tailLength, tailLength, target);
        } else {
            long tail = memorySegment.get(JAVA_LONG_UNALIGNED, endIndex - ALIGNMENT_INT);
            if (length < ALIGNMENT) {
                long adjustedTail = tail >> (ALIGNMENT_INT - tailLength) * ALIGNMENT_INT;
                Bits.transferLimitedDataTo(adjustedTail, length - tailLength, tailLength, target);
            } else {
                Bits.transferDataTo(tail, length - ALIGNMENT_INT, target);
            }
        }

        int longsBetween = length - tailLength - headLength;
        long headBump = alignedStart + (headOffset > 0 ? ALIGNMENT_INT : 0);
        for (int index = 0; index < longsBetween; index += ALIGNMENT_INT) {
            Bits.transferDataTo(
                memorySegment.get(
                    JAVA_LONG,
                    headBump + index
                ),
                headLength + index,
                target
            );
        }
        return target;
    }

    private static long readTail(
        LineSegment segment,
        MemorySegment memorySegment,
        int length,
        long endIndex,
        long tailLen,
        boolean truncate
    ) {
        if (length < ALIGNMENT) {
            return segment.tail(truncate);
        }
        long data = memorySegment.get(JAVA_LONG_UNALIGNED, endIndex - ALIGNMENT);
        long shift = ALIGNMENT * (ALIGNMENT - tailLen);
        return data >> shift;
    }

    private static final class LineSegmentAlignedLongSupplier implements LongSupplier {

        private final long alignedStart;

        private final int headLen;

        private final long endIndex;

        private final int tailLen;

        private final LineSegment segment;

        private final int length;

        private long position;

        private final MemorySegment memorySegment;

        private final long alignedEnd;

        private LineSegmentAlignedLongSupplier(LineSegment segment, int length) {
            this.segment = segment;
            this.memorySegment = segment.memorySegment();
            this.length = length;
            this.alignedStart = this.segment.alignedStart();
            this.alignedEnd = this.segment.alignedEnd();
            this.headLen = this.segment.headLength();
            this.endIndex = this.segment.endIndex();
            this.tailLen = Math.toIntExact(this.endIndex % ALIGNMENT);

            this.position = this.alignedStart;
        }

        @Override
        public long getAsLong() {
            if (position == alignedStart && headLen > 0) {
                try {
                    return segment.head(true);
                } finally {
                    position += ALIGNMENT;
                }
            }
            if (position < alignedEnd) {
                try {
                    return memorySegment.get(JAVA_LONG, position);
                } finally {
                    position += ALIGNMENT;
                }
            }
            if (position == alignedEnd && tailLen > 0) {
                try {
                    return readTail(segment, memorySegment, length, endIndex, tailLen, true);
                } finally {
                    position += ALIGNMENT;
                }
            }
            return 0x0L;
        }
    }

    private static final class LineSegmentShiftedLongSupplier implements LongSupplier {

        private final LineSegment segment;

        private final int length;

        private final long endIndex;

        private final int tailLen;

        private final int headShift;

        private final int tailShift;

        private final long alignedEnd;

        private long position;

        private long data;

        private final MemorySegment memorySegment;

        private LineSegmentShiftedLongSupplier(LineSegment segment, int length, int headLen) {
            this.segment = segment;
            this.memorySegment = segment.memorySegment();
            this.length = length;
            this.endIndex = segment.endIndex();
            this.tailLen = Math.toIntExact(endIndex % ALIGNMENT);

            this.headShift = Math.toIntExact(headLen * ALIGNMENT);
            this.tailShift = Math.toIntExact((ALIGNMENT - headLen) * ALIGNMENT);

            this.alignedEnd = segment.alignedEnd();

            this.data = segment.head(true);
            this.position = segment.alignedStart() + (headLen > 0 ? ALIGNMENT : 0);
        }

        @Override
        public long getAsLong() {
            if (length < ALIGNMENT) {
                return data;
            }
            if (position < alignedEnd) {
                long alignedData = memorySegment.get(JAVA_LONG, position);
                try {
                    data |= alignedData << headShift;
                    return data;
                } finally {
                    data = alignedData >> tailShift;
                    position += ALIGNMENT;
                }
            }
            if (position == this.alignedEnd && tailLen > 0) {
                try {
                    long alignedData = readTail(segment, memorySegment, length, endIndex, tailLen, true);
                    data |= alignedData << headShift;
                    return data;
                } finally {
                    position += ALIGNMENT;
                }
            }
            return 0x0L;
        }
    }
}
