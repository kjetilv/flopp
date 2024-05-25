package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;
import com.github.kjetilv.flopp.kernel.bits.MemorySegments;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class LineSegmentsTest {

    @Test
    void longs() {
        String string = "1234abcdabcd5678";
        LongStream longs = LineSegments.of(string, UTF_8).alignedLongStream();
        String str = streamed(longs);
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
        assertLongs("___12345678abcdefghABCDEFGH__", 3, 27);
        assertLongs("___12345678abcdefgh__", 3, 19);
        assertLongs("___12345678__", 3, 11);
        assertLongs("_sdf__", 1, 4);
        assertLongs("_____", 3, 3);
    }

    private static void assertLongs(String string, int startIndex, int endIndex) {
        LineSegment slice = LineSegments.of(string, UTF_8).slice(startIndex, endIndex);

        LongStream longStream = slice.longStream(true);
        LongSupplier longSupplier = slice.longSupplier(true);

        String streamString = streamed(longStream);
        String supplierString = supplied(longSupplier, slice.alignedLongsCount() - 1);

        String substring = string.substring(startIndex, endIndex);

        String pad = !substring.isEmpty() && substring.length() < 8
            ? Bits.toString(0, 8 - substring.length(), UTF_8)
            : "";

        assertThat(streamString).isEqualTo(substring + pad);
        assertThat(supplierString).isEqualTo(streamString);
//        String longBytes = new String(LineSegments.fromLongBytes(slice), StandardCharsets.UTF_8);
//        assertThat(longBytes).isEqualTo(substring);
    }

    private static String streamed(LongStream longStream) {
        return longStream.mapToObj(l -> Bits.toString(l, UTF_8))
            .collect(Collectors.joining());
    }

    private static String supplied(LongSupplier longSupplier, long length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            String bits = Bits.toString(longSupplier.getAsLong(), UTF_8);
            sb.append(bits);
        }
        return sb.toString();
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

    private static void assertTail(String string) {
        LineSegment lineSegment = LineSegments.of(string, UTF_8);
        int tail = string.length() - 8;
        long l = MemorySegments.readTail(lineSegment.memorySegment(), lineSegment.length(), tail);
        assertThat(Bits.toString(l, tail, UTF_8)).isEqualTo(string.substring(8));
    }

    private static void assertHead(String string) {
        LineSegment lineSegment = LineSegments.of(string, UTF_8);
        int head = string.length() - 8;
        long l = MemorySegments.readHead(lineSegment.memorySegment(), 0, head);
        assertThat(Bits.toString(l, head, UTF_8)).isEqualTo(string.substring(0, head));
    }
}
