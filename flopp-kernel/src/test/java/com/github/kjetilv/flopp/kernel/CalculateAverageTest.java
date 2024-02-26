package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CalculateAverageTest {

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
                Map<String, CalculateAverage_kjetilvlong.Result> map = CalculateAverage_kjetilvlong.go(
                    smaple,
                    Partitioning.single(),
                    Shape.of(smaple).longestLine(128)
                );
                Path out = Path.of(smaple.toString().replace(".txt", ".out"));
                assertThat(out).content()
                    .isEqualTo(
                        STR."""
                        \{map}
                        """)
                    .describedAs(smaple.toString());
            });
        }
    }

    @Test
    void splitAll() throws IOException {
        Path path = Path.of(
            Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("smaples"))
                .getFile());
        try (
            Stream<Path> smaples = Files.list(path)
                .filter(smaple -> smaple.toString().endsWith(".txt"))
        ) {
            smaples.forEach(smaple -> {
                LongAdder map = JustSplit_kjetilvlong.add(
                    Partitioning.single(),
                    Shape.of(smaple).longestLine(128),
                    smaple
                );
                try (Stream<String> lines = Files.lines(smaple)) {
                    assertThat(map).hasValue(lines.count() * 2);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
    }

}
