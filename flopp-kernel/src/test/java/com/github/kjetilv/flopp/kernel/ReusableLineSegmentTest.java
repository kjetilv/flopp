package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

class ReusableLineSegmentTest {

    @Test
    void shouldMorph(){
        ReusableLineSegment reusableLineSegment = new ReusableLineSegment(33);

        reusableLineSegment.setLength(9);
        reusableLineSegment.setLong(0, 0x4849505152535455L);
        reusableLineSegment.setLong(1, 50L);
        System.out.println(reusableLineSegment.asString(UTF_8));
    }

}
