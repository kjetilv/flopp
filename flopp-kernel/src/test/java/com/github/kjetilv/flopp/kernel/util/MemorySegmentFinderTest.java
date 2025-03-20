package com.github.kjetilv.flopp.kernel.util;

import com.github.kjetilv.flopp.kernel.MemorySegments;
import com.github.kjetilv.flopp.kernel.Vectors;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.function.LongSupplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class MemorySegmentFinderTest {

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
        LongSupplier longSupplier = Vectors.finder(memorySegment, (byte) '\n');

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
        Vectors.Finder finder = Vectors.finder(memorySegment, 6, (byte) '\n');

        assertThat(finder.next()).isEqualTo(6L);
        assertThat(finder.next()).isEqualTo(9L);
        assertThat(finder.next()).isEqualTo(14L);
        assertThat(finder.next()).isEqualTo(20L);
        assertThat(finder.next()).isEqualTo(24L);
        assertThat(finder.next()).isEqualTo(-1L);
    }

}