package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.partitions.Partitioning;
import com.github.kjetilv.flopp.kernel.partitions.Partitionings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class FastPartitionerTest {

    @TempDir
    private Path tmp;

    @Test
    void test(TestInfo testInfo) {
        for (int i = 50; i < 200; i++) {
            for (int p = 1; p < 40; p++) {
                try {
                    run(testInfo, i, p);
                } catch (Exception e) {
                    throw new IllegalStateException("Failed " + i + " lines with " + p + " partitions", e);
                }
            }
        }
    }

    @Test
    void tes2(TestInfo testInfo) throws IOException {
        run2(testInfo, 80, 7);
    }

    @Test
    void tes3(TestInfo testInfo) throws IOException {
        run3(testInfo, 80, 7);
    }

    @Test
    void tes3a(TestInfo testInfo) throws IOException {
        run3(testInfo, 100, 2);
    }

    @Test
    void test3(TestInfo testInfo) {
        for (int i = 100; i < 300; i++) {
            for (int p = 2; p < 45; p++) {
                try {
                    run3(testInfo, i, p);
                } catch (Exception e) {
                    throw new IllegalStateException("Failed " + i + " lines with " + p + " partitions", e);
                }
            }
        }
    }

    private void run(TestInfo testInfo, int lineCount, int partitionCount) throws IOException {
        Method method = testInfo.getTestMethod().orElseThrow();

        Path file = FileBuilder.file(
            tmp,
            method.getName(),
            lineCount,
            4,
            new Shape.Decor(1, 1)
        );

        Shape shape = Shape.size(Files.size(file), UTF_8).headerFooter(1, 1).longestLine(128);
        LongAdder cont = new LongAdder();
        try (
            Partitioned partitioned = PartitionedPaths.partitioned(
                file,
                Partitionings.create(partitionCount, 10),
                shape
            )
        ) {
            partitioned.streamers()
                .forEach(streamer ->
                    streamer.lines()
                        .forEach(_ ->
                            cont.increment()));
            assertThat(cont).hasValue(lineCount);
        }
    }

    private void run2(TestInfo testInfo, int lineCount, int partitionCount) throws IOException {
        Method method = testInfo.getTestMethod().orElseThrow();

        Path file = FileBuilder.file(
            tmp,
            method.getName(),
            lineCount,
            4,
            new Shape.Decor(1, 1)
        );

        Shape shape = Shape.size(Files.size(file), UTF_8).longestLine(32).headerFooter(1, 1);
        LongAdder count;
        try (
            Partitioned partitioned = PartitionedPaths.partitioned(
                file,
                Partitionings.create(partitionCount, 10),
                shape
            )
        ) {
            count = new LongAdder();
            BiConsumer<Partition, Stream<LineSegment>> consumer = (_, entries) ->
                entries.forEach(_ -> count.increment());
            partitioned.streamers()
                .map(partitionStreamer ->
                    new PartitionResult<>(
                        partitionStreamer.partition(), () -> {
                        consumer.accept(partitionStreamer.partition(), partitionStreamer.lines());
                        return null;
                    }
                    ))
                .toList()
                .forEach(PartitionResult::complete);
        }
        assertThat(count).hasValue(lineCount);
    }

    private void run3(TestInfo testInfo, int lineCount, int partitionCount) throws IOException {
        Method method = testInfo.getTestMethod().orElseThrow();

        Path file = FileBuilder.file(
            tmp,
            method.getName(),
            lineCount,
            4,
            new Shape.Decor(1, 1)
        );
        int longestLine;
        try (Stream<String> lines = Files.lines(file)) {
            longestLine = lines.max(Comparator.comparingInt(String::length))
                .map(String::length)
                .orElseThrow();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Partitioning partitioning = Partitionings.create(partitionCount, longestLine);
        Shape shape = Shape.size(Files.size(file), UTF_8).longestLine(longestLine).headerFooter(1, 1);
        try (
            Partitioned partitioned = PartitionedPaths.partitioned(file, partitioning, shape)
        ) {
            LongAdder cont = new LongAdder();
            partitioned.streamers()
                .forEach(bitwisePartitionStreamer ->
                    bitwisePartitionStreamer.lines()
                        .forEach(_ -> {
                            cont.increment();
//                            System.out.println(LineSegments.asString(l));
                        }));
            assertThat(cont)
                .describedAs(lineCount + " lineCount, " + partitionCount + " partitionCount")
                .hasValue(lineCount);
        }
    }
}
