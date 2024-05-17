package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Partition;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BitwisePartitionHandlerTest {

    @Test
    void shortPartition9() {
        parsedOK("abcd;1234");
    }

    @Test
    void shortPartition() {
        parsedOK("abcd;123");
    }

    @Test
    void shortPartition7() {
        parsedOK("abc;123");
    }

    @Test
    void shortPartition2() {
        parsedOK("ab");
    }

    private static void parsedOK(String str) {
        List<LineSegment> handled = new ArrayList<>();
        MemorySegment memorySegment = MemorySegments.of(str);
        BitwisePartitionHandler handler = new BitwisePartitionHandler(
            new Partition(0, 1, 0, str.length()),
            memorySegment,
            (segment, startIndex, endIndex) ->
                handled.add(LineSegments.of(segment, startIndex, endIndex)),
            () -> null
        );
        handler.run();
        assertThat(handled).singleElement().satisfies(lineSegment ->
            assertThat(lineSegment.asString()).isEqualTo(str));
    }
}
