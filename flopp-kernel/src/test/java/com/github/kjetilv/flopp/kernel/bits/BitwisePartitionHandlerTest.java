package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Partition;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
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
        MemorySegment memorySegment = MemorySegments.of(str, UTF_8);
        BitwisePartitionHandler handler = new BitwisePartitionHandler(
            new Partition(0, 1, 0, str.length()),
            memorySegment,
            str.length(),
            (segment, startIndex, endIndex) ->
                handled.add(LineSegments.of(segment, startIndex, endIndex)),
            () -> null
        );
        handler.run();
        assertThat(handled).singleElement().satisfies(lineSegment ->
            assertThat(lineSegment.asString(UTF_8)).isEqualTo(str));
    }
}
