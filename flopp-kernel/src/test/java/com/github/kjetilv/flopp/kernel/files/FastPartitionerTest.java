package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

public class FastPartitionerTest {

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

    private static void run(TestInfo testInfo, int lineCount, int partitionCount) throws IOException {

        Method method = testInfo.getTestMethod().orElseThrow();

        Path file = FileBuilder.file(
            Files.createTempDirectory(method.getName()),
            method.getName(),
            lineCount,
            4,
            new Shape.Decor(1, 1)
        );

        try {
            Shape shape = Shape.size(Files.size(file)).header(1, 1);
            try (
                PartitionedPath partitioned = new DefaultPartitionedPath(
                    file,
                    shape,
                    Partitioning.create(partitionCount, 10),
                    new FileChannelSources(file, shape.size()),
                    Executors.newVirtualThreadPerTaskExecutor()
                )
            ) {
                LongAdder cont = new LongAdder();
                partitioned.streams().streamers()
                    .forEach(streamer ->
                        streamer.nLines()
                            .forEach(line ->
                                cont.increment()));
                assertThat(cont).hasValue(lineCount);
            }
        } finally {
            Files.delete(file);
        }
    }

    private static void run2(TestInfo testInfo, int lineCount, int partitionCount) throws IOException {
        Method method = testInfo.getTestMethod().orElseThrow();

        Path file = FileBuilder.file(
            Files.createTempDirectory(method.getName()),
            method.getName(),
            lineCount,
            4,
            new Shape.Decor(1, 1)
        );

        try {
            Shape shape = Shape.size(Files.size(file)).longestLine(32).header(1, 1);
            try (
                Partitioned<Path> partitioned = new DefaultPartitioned<>(
                    file,
                    shape,
                    Partitioning.create(partitionCount, 10),
                    new FileChannelSources(file, shape.size()),
                    Executors.newVirtualThreadPerTaskExecutor()
                )
            ) {
                LongAdder cont = new LongAdder();
                partitioned.consumer().forEachNLine(
                        (partition, entries) ->
                            entries.forEach(line -> cont.increment())
                    )
                    .toList()
                    .forEach(CompletableFuture::join);
                assertThat(cont).hasValue(lineCount);
            }
        } finally {
            Files.delete(file);
        }
    }
}
