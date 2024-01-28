package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.bits.BitwisePartitionStreamers;
import com.github.kjetilv.flopp.kernel.bits.LineSegments;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class FastPartitionerTest {

    @TempDir
    Path tmp;

    @Test
    void test(TestInfo testInfo) {
        for (int i = 50; i < 200; i++) {
            for (int p = 1; p < 40; p++) {
                try {
                    run(testInfo, i, p);
                } catch (Exception e) {
                    throw new IllegalStateException(STR."Failed \{i} lines with \{p} partitions", e);
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
                    throw new IllegalStateException(STR."Failed \{i} lines with \{p} partitions", e);
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

        Shape shape = Shape.size(Files.size(file)).header(1, 1);
        LongAdder cont = new LongAdder();
        try (
            PartitionedPath partitioned = new DefaultPartitionedPath(
                file,
                shape,
                new Partitioning(partitionCount, 10),
                new FileSources(file, shape, 1024),
                Executors.newVirtualThreadPerTaskExecutor()
            );
            PartitionedStreams streams = partitioned.streams()
        ) {
            streams.streamers()
                .forEach(streamer ->
                    streamer.nLines()
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

        Shape shape = Shape.size(Files.size(file)).longestLine(32).header(1, 1);
        try (
            Partitioned<Path> partitioned = new DefaultPartitioned<>(
                file,
                shape,
                new Partitioning(partitionCount, 10),
                new FileSources(file, shape, 1024),
                Executors.newVirtualThreadPerTaskExecutor()
            )
        ) {
            LongAdder cont = new LongAdder();
            partitioned.consumer().forEachNLine(
                    (_, entries) ->
                        entries.forEach(_ -> cont.increment())
                )
                .toList()
                .forEach(CompletableFuture::join);
            assertThat(cont).hasValue(lineCount);
        }
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

        Partitioning partitioning = Partitioning.longAligned(partitionCount, longestLine);
        Shape shape = Shape.size(Files.size(file)).longestLine(longestLine).header(1, 1);
        try (
            BitwisePartitionStreamers bitwisePartitionStreamers =
                new BitwisePartitionStreamers(file, shape, partitioning.of(shape.size()))
        ) {
            LongAdder cont = new LongAdder();
            bitwisePartitionStreamers.streamers()
                .forEach(bitwisePartitionStreamer ->
                    bitwisePartitionStreamer.lines()
                        .forEach(l -> {
                            cont.increment();
                            System.out.println(LineSegments.toString(l));
                        }));
            assertThat(cont)
                .describedAs(STR."lineCount \{lineCount} partitionCount \{partitionCount}")
                .hasValue(lineCount);
        }
    }
}
