package com.github.kjetilv.lopp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import static org.assertj.core.api.Assertions.assertThat;

public class FastPartitionerTest {

    @Test
    void test(TestInfo testInfo) throws IOException {
        for (int i = 50; i < 300; i++) {
            for (int p = 1; p < 50; p++) {
                try {
                    run(testInfo, i, p);
                } catch (Exception e) {
                    throw new IllegalStateException("Failed " + i + " lines with " + p + " partitions", e);
                }
            }
        }
    }

    private static void run(TestInfo testInfo, int lineCount, int partitionCount) throws IOException {
        FileShape fileShape = FileShape.base().header(1, 1);

        Method method = testInfo.getTestMethod().orElseThrow();

        Path file = FileBuilder.file(
            Files.createTempDirectory(method.getName()),
            method.getName(),
            fileShape,
            lineCount,
            4
        );

        try {
            FileShape shape = fileShape.fileSize(Files.size(file)).longestLine(32);

                try (
                PartitionedFile partitionedFile = new FastPartitionedFile(
                    file,
                    shape,
                    Partitioning.create(partitionCount, 10),
                    ForkJoinPool.commonPool()
                )
            ) {
                List<NPLine> lines = new ArrayList<>();
                partitionedFile.streamers()
                    .forEach(streamer ->
                        streamer.lines()
                            .forEach(lines::add));
                assertThat(lines).hasSize(lineCount + 2);
            }
        } finally {
            Files.delete(file);
        }
    }
}
