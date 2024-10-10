package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.files.PartitionedPaths;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionedTest {

    @Test
    void testWithHeader() throws IOException {
        Path pathWithHeaders = Files.createTempFile(UUID.randomUUID().toString(), ".test");
        Files.write(
            pathWithHeaders,
            Arrays.asList("""
                HEADER
                1a
                1aa
                1aaa
                2aaaaaa
                3bbbb
                4bbbbbbbb
                5ccccc
                6cccccccccc
                7dddddd
                8dddddddddddd
                9eeeeee
                10
                
                11
                """.split("\n"))
        );
        Shape shape = Shape.size(Files.size(pathWithHeaders), UTF_8).header(1).longestLine(10);

        List<String> syncLines = new ArrayList<>();

        Partitioning partitioning = Partitioning.create(4, 16);
        try (
            Partitioned<Path> pf1 = PartitionedPaths.partitioned(pathWithHeaders, partitioning, shape)
        ) {
            pf1.streamers()
                .forEach(partitionStreamer ->
                    partitionStreamer.lines()
                        .map(lineSegment -> lineSegment.asString(UTF_8))
                        .forEach(syncLines::add));
        }
        assertContents(syncLines);


        List<String> asyncLines = new ArrayList<>();

        try (
            Partitioned<Path> pf2 = PartitionedPaths.partitioned(pathWithHeaders, partitioning, shape);
            ExecutorService executorService = Executors.newFixedThreadPool(10)
        ) {
            pf2.streamers()
                .map(streamer ->
                    CompletableFuture.supplyAsync(streamer::lines, executorService))
                .map(future ->
                    future.thenAccept(partitionedLineStream ->
                        partitionedLineStream.map(lineSegment -> lineSegment.asString(UTF_8))
                            .forEach(asyncLines::add)))
                .forEach(CompletableFuture::join);
        }
        assertContents(asyncLines);
    }

    @Test
    void testWithHeaders() throws IOException {
        Path pathWithHeaders = Files.createTempFile(UUID.randomUUID().toString(), ".test");
        Files.write(
            pathWithHeaders,
            Arrays.asList("""
                HEADER
                AND HEADER
                MORE
                1a
                1aa
                1aaa
                2aaaaaa
                3bbbb
                4bbbbbbbb
                5ccccc
                6cccccccccc
                7dddddd
                8dddddddddddd
                9eeeeee
                10
                
                11
                FOOTER1
                FOOTER2
                """.split("\n"))
        );
        Shape shape = Shape.size(Files.size(pathWithHeaders), UTF_8).headerFooter(3, 2);

        List<String> syncLines = new ArrayList<>();
        int partitionCount = 2; //Runtime.getRuntime().availableProcessors();
        Partitioning partitioning = Partitioning.create(partitionCount, 16);

        try (
            Partitioned<Path> pf1 = PartitionedPaths.partitioned(pathWithHeaders, partitioning, shape)
        ) {
            pf1.streamers()
                .forEach(partitionStreamer ->
                    partitionStreamer.lines()
                        .map(lineSegment -> lineSegment.asString(UTF_8))
                        .forEach(syncLines::add));
        }
        assertContents(syncLines);

        List<String> asyncLines = new ArrayList<>();
        try (
            Partitioned<Path> pf2 = PartitionedPaths.partitioned(pathWithHeaders, partitioning, shape);
            ExecutorService executorService = Executors.newFixedThreadPool(10)
        ) {
            pf2.streamers()
                .map(streamer ->
                    CompletableFuture.supplyAsync(streamer::lines, executorService))
                .map(future ->
                    future.thenAccept(partitionedLineStream ->
                        partitionedLineStream.map(lineSegment -> lineSegment.asString(UTF_8))
                            .forEach(asyncLines::add)))
                .forEach(CompletableFuture::join);
        }
        assertContents(asyncLines);
    }

    @Test
    void testWithoutHeaders() throws IOException {
        Path pathWithHeaders = Files.createTempFile(UUID.randomUUID().toString(), ".test");
        Files.write(
            pathWithHeaders,
            Arrays.asList("""
                1a
                1aa
                1aaa
                2aaaaaa
                3bbbb
                4bbbbbbbb
                5ccccc
                6cccccccccc
                7dddddd
                8dddddddddddd
                9eeeeee
                10
                
                11
                """.split("\n"))
        );
        Shape shape = Shape.size(Files.size(pathWithHeaders), UTF_8);

        List<String> syncLines = new ArrayList<>();

        Partitioning partitioning = Partitioning.create(4, 16);
        try (
            Partitioned<Path> pf1 = PartitionedPaths.partitioned(pathWithHeaders, partitioning, shape)
        ) {
            pf1.streamers()
                .forEach(partitionStreamer ->
                    partitionStreamer.lines()
                        .map(lineSegment -> lineSegment.asString(UTF_8))
                        .forEach(syncLines::add)
                );
        }
        assertContents(syncLines);

        List<String> asyncLines = new ArrayList<>();
        try (
            Partitioned<Path> pf2 = PartitionedPaths.partitioned(pathWithHeaders, partitioning, shape);
            ExecutorService executorService = Executors.newFixedThreadPool(10)
        ) {
            pf2.streamers()
                .map(streamer ->
                    CompletableFuture.supplyAsync(streamer::lines, executorService))
                .map(future ->
                    future.thenAccept(partitionedLineStream ->
                        partitionedLineStream.map(lineSegment -> lineSegment.asString(UTF_8))
                            .forEach(asyncLines::add)))
                .forEach(CompletableFuture::join);
        }
        assertContents(asyncLines);
    }

    private static void assertContents(List<String> lines) {
        String collect = lines.stream()
            .map(Object::toString)
            .collect(Collectors.joining("\n"));
        assertEquals(14, lines.size(),
            collect
        );
        assertEquals("1a", lines.getFirst(), collect);
        assertEquals("11", lines.get(13), collect);
    }
}
