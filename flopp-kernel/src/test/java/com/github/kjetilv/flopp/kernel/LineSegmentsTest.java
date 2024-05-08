package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

class LineSegmentsTest {

    @Test
    void longs() {
        String string = "1234abcdabcd5678";
        LongStream longs = LineSegments.of(string).unalignedLongs();
        String str = longs.mapToObj(Bits::toString).collect(Collectors.joining());
        assertThat(str).isEqualTo(string);
    }

    @Test
    void misalignedLongs() {
        assertLongs("___12345678abcdefghABCDEFGH__", 3, 27);
        assertLongs("___12345678abcdefgh__", 3, 19);
        assertLongs("___12345678__", 3, 11);
        assertLongs("_sdf__", 1, 4);
        assertLongs("_____", 3, 3);
    }

    private static void assertLongs(String string, int startIndex, int endIndex) {
        LongStream longs = LineSegments.of(string).slice(startIndex, endIndex).longs(true);
        String str = longs.mapToObj(Bits::toString).collect(Collectors.joining());
        String substring = string.substring(startIndex, endIndex);
        String pad = !substring.isEmpty() && substring.length() < 8 ? Bits.toString(0, 8 - substring.length()) : "";
        assertThat(str).isEqualTo(substring + pad);
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
        LineSegment lineSegment = LineSegments.of(string);
        int tail = string.length() - 8;
        long l = MemorySegments.readTail(lineSegment.memorySegment(), lineSegment.length(), tail);
        assertThat(Bits.toString(l, tail)).isEqualTo(string.substring(8));
    }

    private static void assertHead(String string) {
        LineSegment lineSegment = LineSegments.of(string);
        int head = string.length() - 8;
        long l = MemorySegments.readHead(lineSegment.memorySegment(), 0, head);
        assertThat(Bits.toString(l, head)).isEqualTo(string.substring(0, head));
    }
}
