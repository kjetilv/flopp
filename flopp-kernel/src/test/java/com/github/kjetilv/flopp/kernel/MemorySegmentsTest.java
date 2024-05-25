package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.MemorySegments;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

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

}
