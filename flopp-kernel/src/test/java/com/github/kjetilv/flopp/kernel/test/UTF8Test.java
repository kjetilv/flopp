package com.github.kjetilv.flopp.kernel.test;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.bits.Bitwise;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class UTF8Test {

    @Test
    void nonTerminated() {
        assertNonTerminated("dotted.txt", 2, 0);
        assertNonTerminated("dotted.txt", 3, 0);
    }

    @Test
    void nonTerminatedXyz2() {
        for (int i = 3; i < 8; i++) {
            assertNonTerminated("whoopsei.txt", i, 0);
        }
    }

    @Test
    void nonTerminatedXyz7parts() {
        assertNonTerminated("whoopsei.txt", 7, 0);
    }

    @Test
    void mergeMultis() {
        for (int partitions = 1; partitions < 10; partitions++) {
            for (int tail = 0; tail < 20; tail++) {
                assertNonTerminated("mergemulti.txt", partitions, tail);
            }
        }
    }

    @Test
    void mergeMultisUTF8() {
        for (int partitions = 1; partitions < 20; partitions++) {
            for (int tail = 0; tail < 25; tail++) {
                assertNonTerminated("mergemulti-utf8.txt", partitions, tail);
            }
        }
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
                Shape.of(path, UTF_8).longestLine(20)
            )
        ) {
            sb = new StringBuilder();
            try {
                extract(bitwisePartitioned.streams(), sb);
            } catch (Exception e) {
                System.err.println("\n\n###\n\n" + linesOf(path));
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
            Partitioned<Path> partitioned = Bitwise.partititioned(path)
        ) {
            sb = new StringBuilder();
            try {
                extract(partitioned.streams(), sb);
            } catch (Exception e) {
                System.err.println("\n\n###\n\n" + linesOf(path));
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
                Shape.of(path, UTF_8).longestLine(110)
            )
        ) {
            sb = new StringBuilder();
            try {
                extract(partititioned.streams(), sb);
            } catch (Exception e) {
                System.err.println("\n\n###\n\n" + linesOf(path));
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
                Partitioning.create(2, 20),
                Shape.of(path, UTF_8).longestLine(512)
            )
        ) {
            sb = new StringBuilder();
            extract(bitwisePartitioned.streams(), sb);
        }
    }

    @Test
    void mystery() {
        Path path = path("measurements-complex-utf8.txt");
        StringBuilder sb;
        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(
                path,
                Partitioning.create(2, 0),
                Shape.of(path, UTF_8).longestLine(30)
            )
        ) {
            sb = new StringBuilder();
            extract(bitwisePartitioned.streams(), sb);
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
                Shape.of(path, UTF_8).longestLine(40)
            )
        ) {
            sb = new StringBuilder();
            try {
                extract(bitwisePartitioned.streams(), sb);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        assertThat(path).content()
            .isEqualTo(sb.toString());
    }

    private static final Pattern LN = Pattern.compile("\n");

    private static void assertNonTerminated(String file, int partitionCount, int longestLine) {
        Path path = path(file);
        Path tmp;
        try {
            tmp = Files.createTempFile(UUID.randomUUID().toString(), ".txt");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String trimmedContent;
        try (
            Stream<String> lines = Files.lines(path, UTF_8)
        ) {
            trimmedContent = lines.collect(Collectors.joining("\n"));
            Files.write(tmp, (trimmedContent.trim() + "\n").getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        StringBuilder sb = new StringBuilder();
        Partitioning partitioning = Partitioning.create(partitionCount);
        Shape shape = Shape.of(tmp, UTF_8).longestLine(longestLine);
        Partitions partitions = partitioning.of(shape.size());
        try (
            Partitioned<Path> partitioned = Bitwise.partititioned(tmp, partitioning)
        ) {
            partitioned.streams().streamers()
                .forEach(partitionStreamer ->
                    partitionStreamer.lines()
                        .forEach(line -> {
                                String string = line.asString(UTF_8);
//                                System.out.println(string);
                                sb.append(string).append("\n");
                            }
                        ));
        }
        assertThat(tmp).content(UTF_8)
            .describedAs("Failed with " + partitionCount + " partitions: " + partitions)
            .isEqualTo(sb.toString());
    }

    private static String linesOf(Path path) {
        try (Stream<String> lines = Files.lines(path)) {
            return lines.collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Path path(String name) {
        URL resource = Objects.requireNonNull(
            Thread.currentThread().getContextClassLoader().getResource(name), "resource: " + name
        );
        return Path.of(Objects.requireNonNull(resource.getFile(), "resource.getFile()"));
    }

    private static void extract(
        PartitionedStreams streams,
        StringBuilder sb
    ) {
        List<String> strings = streams.streamers()
            .flatMap(partitionStreamer ->
                partitionStreamer.lines()
                    .map(lineSegment -> lineSegment.asString(UTF_8))
                    .map(line ->
                        LN.matcher(line).replaceAll("🦊")))
            .toList();
        boolean foxed = strings.stream().anyMatch(string -> string.contains("🦊"));
        strings.forEach(str -> {
            sb.append(str).append("\n");
            if (foxed) {
                System.out.println(str);
            }
        });
    }
}
