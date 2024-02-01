package com.github.kjetilv.flopp.kernel.test;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.PartitionedStreams;
import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitioned;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class UTF8Test {

    @Test
    void test() {
        Path path = path("measurements-complex-utf8.txt");
        StringBuilder sb;
        try (
            PartitionedStreams streams = new BitwisePartitioned(
                path,
                Partitioning.longAligned(3, 512),
                Shape.of(path).longestLine(512)
            ).streams()
        ) {
            sb = new StringBuilder();
            try {
                extract(streams, sb);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        assertThat(path).content()
            .isEqualTo(sb.toString());
    }

    @Test
    void test2() {
        Path path = path("a.txt");
        StringBuilder sb;
        try (
            PartitionedStreams streams = new BitwisePartitioned(
                path,
                Partitioning.longAligned(3, 40),
                Shape.of(path).longestLine(40)
            ).streams()
        ) {
            sb = new StringBuilder();
            try {
                extract(streams, sb);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        assertThat(path).content()
            .isEqualTo(sb.toString());
    }

    private static Path path(String name) {
        URL resource = Thread.currentThread().getContextClassLoader().getResource(name);
        Path path = Path.of(resource.getFile());
        return path;
    }

    private static void extract(PartitionedStreams streams, StringBuilder sb) {
        streams.streamers()
            .forEach(partitionStreamer ->
                partitionStreamer.lines()
                    .map(LineSegment::asString)
                    .map(line -> line.replaceAll("\n", "🦊"))
                    .forEach(str -> sb.append(str).append("\n")));
    }
}
