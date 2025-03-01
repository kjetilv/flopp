package com.github.kjetilv.flopp.kernel.util;

import com.github.kjetilv.flopp.kernel.MemorySegments;
import com.github.kjetilv.flopp.kernel.Vectors;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.function.LongSupplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class MemorySegmentByteFinderTest {

    @Test
    void getAsLong() {
        String s = """
            foofoofoo
            foo
            foofoo
            foo
            foo
            
            barbar
            bar
            bar
            
            barbar
            bar
            bar
            
            zotzot
            zot
            zot
            
            zotzot
            zot
            zot
            """;

        MemorySegment memorySegment = MemorySegments.of(s, UTF_8, false);
        LongSupplier longSupplier = new MemorySegmentByteFinder(memorySegment, (byte) '\n');

        for (int i = 0; i < 21; i++) {
            System.out.println(longSupplier.getAsLong());
        }
        assertThat(longSupplier.getAsLong()).isNegative().isEqualTo(-1L);
    }

    @Test
    void getAsLongOffset() {
        String s = """
            f
            fo
            f
            oo
            barb
            barbo
            zot
            zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz""";
        MemorySegment memorySegment = MemorySegment.ofArray(s.getBytes(UTF_8));
        Vectors.ByteFinder byteFinder = new MemorySegmentByteFinder(memorySegment, 6, (byte) '\n');

        assertThat(byteFinder.next()).isEqualTo(6L);
        assertThat(byteFinder.next()).isEqualTo(9L);
        assertThat(byteFinder.next()).isEqualTo(14L);
        assertThat(byteFinder.next()).isEqualTo(20L);
        assertThat(byteFinder.next()).isEqualTo(24L);
        assertThat(byteFinder.next()).isEqualTo(-1L);
    }

}