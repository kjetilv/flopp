package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.segments.LineSegmentTraverser;
import com.github.kjetilv.flopp.kernel.util.Bits;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;

import static com.github.kjetilv.flopp.kernel.MemorySegments.ALIGNMENT_INT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class LineSegmentsTest {

    @Test
    void longs() {
        String string = "1234abcdabcd5678";
        LongStream longs = LineSegments.of(string, UTF_8).longStream(false);
        String str = streamed(longs, 0, string.length());
        assertThat(str).isEqualTo(string);
    }

    @Test
    void longsSupplier() {
        String string = "1234abcdabcd5678";
        LongSupplier longs = LineSegments.of(string, UTF_8).alignedLongSupplier();

        String str = Bits.toString(longs.getAsLong(), UTF_8) + Bits.toString(longs.getAsLong(), UTF_8);
        assertThat(str).isEqualTo(string);
        assertThat(longs.getAsLong()).isZero();
    }

    @Test
    void misalignedLongStream() {
        assertLongs("___12345678abcdefgh__", 3, 19);
        assertLongs("___12345678abcdefgh\\_/\\_///ABCDEFGH__----", 8, 9);
        assertLongs("___12345678__", 3, 11);
        assertLongs("_sdf__", 1, 4);
        assertLongs("_____", 3, 3);

        stressTest("___12345678abcdefgh\\_/\\_///ABCDEFGH__----");
    }

    @Test
    void charStream() {
//        assertLongs("xNgaoundéré;37.8", 3, 19, 14);
        assertLongs("xNgaoundéré;37.8", 1, 13, 10);
        assertLongs("xNgaoundéré;37.8", 1, 11, 9);
        assertLongs("xNgaoundéré;37.8", 2, 11, 8);
        assertLongs("xNgaoundéré;37.8", 2, 10, 7);
        assertLongs("xNgaoundéré;37.8", 2, 8, 6);
    }

    @Test
    void readTail() {
        assertTail("foobarzt");
        assertTail("foobarzt1");
        assertTail("foobarzt12");
        assertTail("foobarzt123");
        assertTail("foobarzt1234");
        assertTail("foobarzt12345");
        assertTail("foobarzt123456");
        assertTail("foobarzt1234567");
    }

    @Test
    void readHead() {
        assertHead("foobarzt");
        assertHead("1foobarzt");
        assertHead("12foobarzt");
        assertHead("123foobarzt");
        assertHead("1234foobarzt");
        assertHead("12345foobarzt");
        assertHead("123456foobarzt");
        assertHead("1234567foobarzt");
    }

    @Test
    void shortOnes() {
        assertHead2("f");
        assertHead2("fo");
        assertHead2("foo");
        assertHead2("foobar");
        assertHead2("foobarz");
    }

    @Test
    void copyTo() {
        LineSegment donor1 = LineSegments.of("foo");
        LineSegment donor2 = LineSegments.of("bar");
        LineSegment receiver = LineSegments.of(128);
        LineSegment copied1 = donor1.copyTo(receiver, 0L);
        LineSegment copied2 = donor2.copyTo(receiver, donor1.length());
        assertThat(donor1).hasSameHashCodeAs(copied1);
        assertThat(donor2).hasSameHashCodeAs(copied2);
        assertThat(receiver.slice(0, donor1.length() + donor2.length()))
            .isEqualTo(LineSegments.of("foobar"));
    }

    private static final LineSegmentTraverser.Reusable REUSABLE = LineSegmentTraverser.create();

    private static final LineSegmentTraverser.Reusable REUSABLE_ALIGNED = LineSegmentTraverser.create(true);

    @SuppressWarnings("SameParameterValue")
    private static void stressTest(String abc) {
        for (int s = 0; s < abc.length(); s++) {
            for (int e = s; e < abc.length(); e++) {
                assertLongs(abc, s, e);
            }
            for (int i = 0; i < s; i++) {
                assertLongs(abc, i, s);
            }
            for (int e = s; e < abc.length(); e++) {
                assertLongs(abc.substring(s, e), 0, e - s);
            }
        }
    }

    private static void assertLongs(String string, int startIndex, int endIndex) {
        assertLongs(string, startIndex, endIndex, endIndex - startIndex);
    }

    private static void assertLongs(String string, int startIndex, int endIndex, int stringLength) {
        LineSegment slice = LineSegments.of(string, UTF_8).slice(startIndex, endIndex);

        String shiftedStreamString;
        String alignedStreamString;
        String shiftSupplierString;
        String alignSupplierString;

        String bitwiseTraversedString;
        String biwiseAlignedTraversedString;

        LineSegmentTraverser.Reusable bitwiseLongTraverse = REUSABLE;
        LineSegmentTraverser.Reusable aligneBitwiseLongTraverse = REUSABLE_ALIGNED;

        String asLongsString;

        try {
            int alignedShift = startIndex % 8;
            int len = endIndex - startIndex;

            LongStream shiftedLongStream = slice.longStream();
            shiftedStreamString = streamed(shiftedLongStream, 0, len);

            LongStream alignedLongStream = slice.alignedLongStream();
            alignedStreamString = streamed(alignedLongStream, alignedShift, alignedShift + len);

            LongSupplier shiftedLongSupplier = slice.longSupplier();
            long shiftedLongsCount = slice.shiftedLongsCount();
            shiftSupplierString = supplied(shiftedLongSupplier, 0, len, shiftedLongsCount);

            LongSupplier alignedLongSupplier = slice.alignedLongSupplier();
            long alignedLongsCount = slice.alignedLongsCount();
            alignSupplierString = supplied(alignedLongSupplier, alignedShift, len, alignedLongsCount);

            long[] longs = LineSegments.asLongs(slice);
            byte[] bytes = Bits.toBytes(longs, len);
            asLongsString = new String(bytes, UTF_8);

            bitwiseLongTraverse = bitwiseLongTraverse.apply(slice);
            bitwiseTraversedString = supplied(bitwiseLongTraverse, 0, len, shiftedLongsCount);

            aligneBitwiseLongTraverse = aligneBitwiseLongTraverse.apply(slice);
            biwiseAlignedTraversedString = supplied(aligneBitwiseLongTraverse, alignedShift, len, alignedLongsCount);

        } catch (Exception e) {
            throw new RuntimeException("Failed in " + startIndex + ", " + endIndex, e);
        }

        String substring = string.substring(startIndex, startIndex + stringLength);

        assertThat(shiftedStreamString)
            .describedAs("Shifted stream produced different string, %s, %s", startIndex, endIndex)
            .isEqualTo(substring);
        assertThat(alignedStreamString)
            .describedAs("Aligned stream produced different string, %s, %s", startIndex, endIndex)
            .isEqualTo(substring);
        assertThat(shiftSupplierString)
            .describedAs("Shifted supplier produced different string, %s, %s", startIndex, endIndex)
            .isEqualTo(shiftedStreamString);
        assertThat(alignSupplierString)
            .describedAs("Aligned supplier produced different string, %s, %s", startIndex, endIndex)
            .isEqualTo(shiftedStreamString);
        assertThat(asLongsString)
            .describedAs("asLongs() produced different string, %s, %s", startIndex, endIndex)
            .isEqualTo(shiftedStreamString);
        assertThat(bitwiseTraversedString)
            .describedAs("bitwiseLongSupplier provided different string, %s, %s", startIndex, endIndex)
            .isEqualTo(substring);
        assertThat(biwiseAlignedTraversedString)
            .describedAs("alignBitwiseLongSupplier provided different string, %s, %s", startIndex, endIndex)
            .isEqualTo(substring);
    }

    private static String streamed(LongStream longStream, int start, int end) {
        byte[] bytes = new byte[64];
        AtomicInteger i = new AtomicInteger();
        longStream.forEach(data -> {
            try {
                if (i.get() == 0) {
                    Bits.transferLimitedDataTo(data, start, ALIGNMENT_INT - start, bytes);
                } else {
                    Bits.transferDataTo(data, i.get(), bytes);
                }
            } finally {
                i.getAndAdd(8);
            }
        });
        return new String(bytes, start, end - start, UTF_8);
    }

    private static String supplied(LongSupplier longSupplier, int start, int end, long count) {
        byte[] bytes = new byte[64];
        AtomicInteger ai = new AtomicInteger();
        for (int i = 0; i < count; i++) {
            long data = longSupplier.getAsLong();
            try {
                if (i == 0 && start > 0) {
                    Bits.transferLimitedDataTo(data, start, ALIGNMENT_INT - start, bytes);
                } else {
                    Bits.transferDataTo(data, ai.get(), bytes);
                }
            } finally {
                ai.getAndAdd(8);
            }
        }
        return new String(bytes, start, end, UTF_8);
    }

    private static void assertTail(String string) {
        LineSegment lineSegment = LineSegments.of(string, UTF_8);
        int tail = string.length() - 8;
        long l = lineSegment.tail();
        assertThat(Bits.toString(l, tail, UTF_8)).isEqualTo(string.substring(8));
    }

    private static void assertHead(String string) {
        LineSegment lineSegment = LineSegments.of(string, UTF_8);
        int head = string.length() - 8;
        long l = lineSegment.head();
        assertThat(Bits.toString(l, head, UTF_8)).isEqualTo(string.substring(0, head));
    }

    private static void assertHead2(String string) {
        LineSegment lineSegment = LineSegments.of(string, UTF_8);
        int head = string.length();
        long l = lineSegment.head();
        assertThat(Bits.toString(l, head, UTF_8)).isEqualTo(string.substring(0, head));
    }
}
