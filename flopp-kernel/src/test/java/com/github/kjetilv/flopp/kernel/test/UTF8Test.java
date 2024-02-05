package com.github.kjetilv.flopp.kernel.test;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.bits.Bitwise;
import com.github.kjetilv.flopp.kernel.bits.LineSegment;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class UTF8Test {

    @Test
    void nonTerminated() throws IOException {
        assertNonTerminated("dotted.txt", 2);
        assertNonTerminated("dotted.txt", 3);
    }

    @Test
    void nonTerminatedXyz2() throws IOException {
        for (int i = 3; i < 8; i++) {
            assertNonTerminated("whoopsei.txt", i);
        }
    }

    @Test
    void nonTerminatedXyz7parts() throws IOException {
        assertNonTerminated("whoopsei.txt", 7);
    }

    @Test
    void testXyz() throws InterruptedException {
        Path path = path("whoopsei.txt");
        StringBuilder sb;
        Partitioning partitioning = Partitioning.create(6, 20);
        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(
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
                System.out.println("\n\n###\n\n" + linesOf(path));
                Thread.sleep(100);
                System.err.println();
                e.printStackTrace(System.err);
            }
        }

        assertThat(path).content()
            .isEqualTo(sb.toString());
    }

    @Test
    void testXyz2() throws InterruptedException {
        Path path = path("whoopsei.txt");
        StringBuilder sb;
        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path);
            PartitionedStreams streams = bitwisePartitioned.streams()
        ) {
            sb = new StringBuilder();
            try {
                extract(streams, sb);
            } catch (Exception e) {
                System.out.println("\n\n###\n\n" + linesOf(path));
                Thread.sleep(100);
                System.err.println();
                e.printStackTrace(System.err);
            }
        }

        assertThat(path).content()
            .isEqualTo(sb.toString());
    }

    @Test
    void test() throws InterruptedException {
        Path path = path("measurements-complex-utf8.txt");
        StringBuilder sb;
        try (
            Partitioned<Path> partititioned = Bitwise.partititioned(
                path,
                Partitioning.create(3, 112),
                Shape.of(path).longestLine(110)
            );
            PartitionedStreams streams = partititioned.streams()
        ) {
            sb = new StringBuilder();
            try {
                extract(streams, sb);
            } catch (Exception e) {
                System.out.println("\n\n###\n\n" + linesOf(path));
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
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(
                path,
                Partitioning.create(4, 20),
                Shape.of(path).longestLine(512)
            );
            PartitionedStreams streams = bitwisePartitioned.streams()
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
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(
                path,
                Partitioning.create(3, 40),
                Shape.of(path).longestLine(40)
            );
            PartitionedStreams streams = bitwisePartitioned.streams()
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

    private static void assertNonTerminated(String file, int partitionCount) throws IOException {
        Path path = path(file);
        Path tmp = Files.createTempFile(UUID.randomUUID().toString(), ".txt");
        String trimmedContent;
        try (
            Stream<String> lines = Files.lines(path)
        ) {
            trimmedContent = lines.collect(Collectors.joining("\n"));
            Files.write(tmp, trimmedContent.trim().getBytes());
        }
        StringBuilder sb = new StringBuilder();
        Partitioning partitioning = Partitioning.create(partitionCount);
        List<Partition> partitions = partitioning.of(Shape.of(tmp).size());
        try (
            Partitioned<Path> partitioned = Bitwise.partititioned(tmp, partitioning)
        ) {
            PartitionedStreams streams = partitioned.streams();
            streams.streamers()
                .forEach(partitionStreamer ->
                    partitionStreamer.lines()
                        .forEach(line -> {
                            String string = line.asString();
                            System.out.println(string);
                            sb.append(string).append("\n");
                            }
                        ));
        }
        assertThat(tmp).content()
            .describedAs(STR."Failed with \{partitionCount} partitions: \{partitions}")
            .isEqualTo(sb.toString().trim());
    }

    private static String linesOf(Path path) {
        try (Stream<String> lines = Files.lines(path)) {
            return lines.collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("DataFlowIssue")
    private static Path path(String name) {
        URL resource = Thread.currentThread().getContextClassLoader().getResource(name);
        Path path = Path.of(Objects.requireNonNull(resource.getFile(), "resource.getFile()"));
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
