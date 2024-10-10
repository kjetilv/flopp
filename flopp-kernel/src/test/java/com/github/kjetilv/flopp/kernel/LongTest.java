package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static com.github.kjetilv.flopp.kernel.util.Bits.hex;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class LongTest {

    @Test
    void test() {
        MemorySegment memorySegment = MemorySegment.ofArray("""
            1
            2a
            3bb
            4c
            e
            
            f
            gh
            0000000
            """.getBytes());
        long l1org = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0L);
        long l1 = l1org;
        long l2org = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 16L);
        long l2 = l2org;

        l1 >>= 8;
        assertThat(l1 & 0x0A)
            .describedAs(hex(l1org) + " " + hex(l1))
            .isEqualTo(0x0A)
        ;
        l1 >>= 8;
        l1 >>= 8;
        l1 >>= 8;
        assertThat(l1 & 0x0A)
            .describedAs(hex(l1org) + " " + hex(l1))
            .isEqualTo(0x0A);
    }
}
