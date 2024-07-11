package com.github.kjetilv.flopp.ca;

import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Shape;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
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
                        test(smaple, partitioning, t);
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
            Shape shape = Shape.of(smaple, UTF_8).longestLine(t);
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
        test(smaple, Partitioning.create(1), 0);
    }

    @Test
    void test2023() {
        test(
            "smaples/measurements-20.txt",
            Partitioning.create(23),
            0
        );
    }

    @SuppressWarnings("unused")
    private static void test(
        String smaple,
        Partitioning partitioning,
        int tail
    ) {
        URL resource = Thread.currentThread().getContextClassLoader().getResource(smaple);
        assertThat(resource).isNotNull();
        test(Path.of(resource.getFile()), partitioning, tail);
    }

    private static void test(
        Path smaple,
        Partitioning partitioning,
        int tail
    ) {
        Shape shape = Shape.of(smaple, UTF_8).longestLine(tail);
        Map<String, CalculateAverage_kjetilvlong.Result> map = CalculateAverage_kjetilvlong.go(
            smaple,
            new CalculateAverage_kjetilvlong.Settings(1, 50, 1.0d, 0.01d, 0.00001d)
        );
        Path out = Path.of(smaple.toString().replace(".txt", ".out"));
        assertThat(out).hasContent(map + "\n")
            .describedAs(smaple + ", " + partitioning + ", " + shape);
    }
}
