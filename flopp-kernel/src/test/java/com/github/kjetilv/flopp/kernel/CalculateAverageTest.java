package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CalculateAverageTest {

    @Disabled
    @Test
    void testAll() throws IOException {
        Path path = Path.of(
            Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("smaples"))
                .getFile());
        try (
            Stream<Path> smaples = Files.list(path)
                .filter(smaple -> smaple.toString().endsWith(".txt"))
        ) {
            smaples.forEach(smaple -> {
                long size;
                try {
                    size = Files.size(smaple);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                int maxPartitions = 4;
                int tail = Math.min(100, Math.toIntExact(size / 10));
                for (int t = 0; t < tail; t += 2) {
                    for (int i = 1; i < maxPartitions; i++) {
                        Partitioning partitioning = Partitioning.create(i);
                        test(smaple, partitioning, t, false);
                    }
                }
            });
        }
    }

    @Test
    void splitAll() throws IOException {
        Path path = Path.of(
            Objects.requireNonNull(
                Thread.currentThread().getContextClassLoader().getResource("smaples")).getFile());
        try (
            Stream<Path> smaples = Files.list(path)
                .filter(smaple ->
                    smaple.toString().endsWith(".txt"))
        ) {
            smaples.forEach(CalculateAverageTest::test);
        }
    }

    @Test
    void splitOne() {
        Path path = Path.of(
            Objects.requireNonNull(
                Thread.currentThread().getContextClassLoader().getResource("smaples/measurements-rounding.txt")).getFile());
        test(path);
    }

    private static void test(Path smaple) {
        long size;
        try {
            size = Files.size(smaple);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        int maxPartitions = Math.min(25, Math.max(1, Math.toIntExact(size / 20)));
        int tail = Math.min(100, Math.toIntExact(size / 10));
        for (int t = 0; t < tail; t += 10) {
            Shape shape = Shape.of(smaple).longestLine(t);
            for (int i = 1; i < maxPartitions; i++) {
                LongAdder sum = JustSplit_kjetilvlong.add(Partitioning.create(i), shape, smaple);
                try (Stream<String> lines = Files.lines(smaple)) {
                    assertThat(sum)
                        .describedAs("smaple: " + t + "/" + i + ": " + smaple)
                        .hasValue(lines.count() * 2);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    @Test
    void test2023() {
        test(
            "measurements-20.txt",
            Partitioning.create(23),
            0,
            true,
            null
//            commaSeparatedLine ->
//                System.out.println(commaSeparatedLine.columns().collect(Collectors.joining(": ")))
        );
    }

    @SuppressWarnings("unused")
    private static void test(
        String smaple,
        Partitioning partitioning,
        int tail,
        boolean slow
    ) {
        test(smaple, partitioning, tail, slow, null);
    }

    private static void test(
        String smaple,
        Partitioning partitioning,
        int tail,
        boolean slow,
        Consumer<SeparatedLine> callbacks
    ) {
        String path = Objects.requireNonNull(
            Thread.currentThread().getContextClassLoader().getResource("smaples/" + smaple), "resource"
        ).getFile();
        test(Path.of(path), partitioning, tail, slow, callbacks);
    }

    private static void test(
        Path smaple,
        Partitioning partitioning,
        int tail,
        boolean slow
    ) {
        test(smaple, partitioning, tail, slow, null);
    }

    private static void test(
        Path smaple,
        Partitioning partitioning,
        int tail,
        boolean slow,
        Consumer<SeparatedLine> callbacks
    ) {
        Shape shape = Shape.of(smaple).longestLine(tail);
        Map<String, CalculateAverage_kjetilvlong.Result> map = CalculateAverage_kjetilvlong.go(
            smaple,
            slow,
            callbacks
        );
        Path out = Path.of(smaple.toString().replace(".txt", ".out"));
        assertThat(out).content()
            .describedAs(smaple + ", " + partitioning + ", " + shape)
            .isEqualTo(map + "\n");
    }
}
