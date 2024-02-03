package com.github.kjetilv.flopp.kernel.test;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.PartitionedStreams;
import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitioned;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class UTF8Test {

    @Test
    void testXyz() throws IOException, InterruptedException {
        Path path = path("whoopsei.txt");
        StringBuilder sb;
        Partitioning partitioning = Partitioning.create(6, 20);
        try (
            BitwisePartitioned bitwisePartitioned = new BitwisePartitioned(
                path,
                partitioning,
                Shape.of(path).longestLine(20)
            );
            PartitionedStreams streams = bitwisePartitioned.streams()
        ) {
            sb = new StringBuilder();
            try {
                extract(streams, sb);
            } catch (Exception e) {
                System.out.println("\n\n###\n\n" + Files.lines(path)
                    .collect(Collectors.joining("\n")));
                Thread.sleep(100);
                System.err.println();
                e.printStackTrace(System.err);
            }
        }

        assertThat(path).content()
            .isEqualTo(sb.toString());
    }

    @Test
    void testXyz2() throws IOException, InterruptedException {
        Path path = path("whoopsei.txt");
        StringBuilder sb;
        try (
            BitwisePartitioned bitwisePartitioned = new BitwisePartitioned(path);
            PartitionedStreams streams = bitwisePartitioned.streams()
        ) {
            sb = new StringBuilder();
            try {
                extract(streams, sb);
            } catch (Exception e) {
                System.out.println("\n\n###\n\n" + Files.lines(path)
                    .collect(Collectors.joining("\n")));
                Thread.sleep(100);
                System.err.println();
                e.printStackTrace(System.err);
            }
        }

        assertThat(path).content()
            .isEqualTo(sb.toString());
    }

    @Test
    void test() throws IOException, InterruptedException {
        Path path = path("measurements-complex-utf8.txt");
        StringBuilder sb;
        try (
            PartitionedStreams streams = new BitwisePartitioned(
                path,
                Partitioning.create(3, 112),
                Shape.of(path).longestLine(110)
            ).streams()
        ) {
            sb = new StringBuilder();
            try {
                extract(streams, sb);
            } catch (Exception e) {
                System.out.println("\n\n###\n\n" + Files.lines(path)
                    .collect(Collectors.joining("\n")));
                Thread.sleep(100);
                System.err.println();
                e.printStackTrace(System.err);
            }
        }

        assertThat(path).content()
            .isEqualTo(sb.toString());
    }

    @Test
    void shortLine() {
        Path path = path("measurements-complex-utf8.txt");
        StringBuilder sb;
        try (
            PartitionedStreams streams = new BitwisePartitioned(
                path,
                Partitioning.create(4, 20),
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

    }

    @Test
    void test2() {
        Path path = path("a.txt");
        StringBuilder sb;
        try (
            PartitionedStreams streams = new BitwisePartitioned(
                path,
                Partitioning.create(3, 40),
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
                    .forEach(str -> {
                        sb.append(str).append("\n");
                        System.out.println(str);
                    }));
    }
}
