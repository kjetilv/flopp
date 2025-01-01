package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class MemorySegmentsTest {

    @Test
    void fromLongsBoundedd() {
        String string = "aaaaabbbCCCCCCCCDDDDDDDDee......";
        //               5+      8       16      24+2

        MemorySegment memorySegment = MemorySegments.of(string, UTF_8);
        String target1 =
            MemorySegments.fromLongsWithinBounds(memorySegment, 5, 26, new byte[40], UTF_8);
        assertThat(target1).isEqualTo(string.substring(5, 26));

        String target2 =
            MemorySegments.fromLongsWithinBounds(memorySegment, 5, 26, null, UTF_8);
        assertThat(target2).isEqualTo(string.substring(5, 26));
    }
    @Test
    void fromLongsOnEdge() {
        String string = "aaaaab.bC...C...D...D...ee..";
        //               5+      8       16      24+2/4

        MemorySegment memorySegment = MemorySegments.of(string, UTF_8);
        String target1 =
            MemorySegments.fromEdgeLong(memorySegment, 5, 26, new byte[40], UTF_8);
        assertThat(target1).isEqualTo(string.substring(5, 26));

        String target2 =
            MemorySegments.fromEdgeLong(memorySegment, 5, 26, null, UTF_8);
        assertThat(target2).isEqualTo(string.substring(5, 26));
    }

    @Test
    void charsTrimSelf() {
        MemorySegments.Chars chars = new MemorySegments.Chars("foo".toCharArray(), 0, 3);
        assertThat(chars.trim()).isSameAs(chars).hasToString("foo");
    }

    @Test
    void charsTrimFront() {
        MemorySegments.Chars chars = new MemorySegments.Chars("   foo".toCharArray(), 0, 6);
        assertThat(chars.trim()).hasToString("foo");
    }

    @Test
    void charsTrimBack() {
        MemorySegments.Chars org = new MemorySegments.Chars("  foo  ".toCharArray(), 2, 5);
        assertThat(org).hasToString("foo  ");
        assertThat(org.trim()).hasToString("foo");
    }

    @Test
    void charsTrimBoth() {
        MemorySegments.Chars chars = new MemorySegments.Chars("   foo  ".toCharArray(), 0, 8);
        assertThat(chars.trim()).hasToString("foo");
    }
}
