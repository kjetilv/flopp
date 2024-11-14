package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.segments.MemorySegments;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class BitwisePartitionLineFeederTest {

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
        BitwisePartitionLineFeeder handler = new BitwisePartitionLineFeeder(
            new Partition(0, 1, 0, str.length()),
            memorySegment,
            0L, str.length(),
            e -> handled.add(e.immutable()),
            () -> null
        );
        handler.run();
        assertThat(handled).singleElement().satisfies(lineSegment ->
            assertThat(lineSegment.asString(UTF_8)).isEqualTo(str));
    }
}
