package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.MemorySegments;
import com.github.kjetilv.flopp.kernel.Partition;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class VectorPartitionLineFeederTest {

    @Test
    void feedsLines() {
        run(false);
    }

    @Test
    void feedsLinesDirect() {
        run(true);
    }

    private static final String TXT = """
        
        0
        
        aaaaaaaaaaaaaaaaaaaaa
        bbbbbbbbbbbbbbbbbbbb
        ccccccccccccccccccc
        ddd
        ee
        f
        
        g
        hh
        iii
        Line 1
        Line 2
        Line 3
        i
        
        Last line
        """;

    private static void run(boolean direct) {
        MemorySegment segment = MemorySegments.of(TXT, UTF_8, direct);
        Partition partition = new Partition(0, 1, 0, segment.byteSize());
        List<String> lines = new ArrayList<>();

        VectorPartitionLineFeeder feeder = new VectorPartitionLineFeeder(
            partition,
            segment,
            0L,
            TXT.length(),
            line ->
                lines.add(line.asString())
        );

        feeder.run();
        assertThat(lines).containsExactly(TXT.split("\n"));
    }
}
