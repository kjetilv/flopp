package com.github.kjetilv.lopp;

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
        Shape shape = Shape.size(Files.size(pathWithHeaders)).header(1);

        List<NpLine> syncLines = new ArrayList<>();

        int partitionCount = Runtime.getRuntime()
            .availableProcessors();
        Partitioning partitioning = new Partitioning(partitionCount, 16);
        Partitioned<Path> pf1 = PartitionedPaths.create(pathWithHeaders, shape, partitioning);
        pf1.streams().streamers()
            .forEach(partitionStreamer ->
                partitionStreamer.lines()
                    .forEach(syncLines::add));
        pf1.close();
        assertContents(syncLines);

        Partitioned<Path> pf2 = PartitionedPaths.create(pathWithHeaders, shape, partitioning);

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        List<NpLine> asyncLines = new ArrayList<>();

        pf2.streams().streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(streamer::lines, executorService))
            .map(future ->
                future.thenAccept(partitionedLineStream ->
                    partitionedLineStream.forEach(asyncLines::add)))
            .forEach(CompletableFuture::join);
        pf2.close();
        assertContents(asyncLines);
    }

    private static void assertContents(List<NpLine> lines) {
        String collect = lines.stream()
            .map(Object::toString)
            .collect(Collectors.joining("\n"));
        assertEquals(14, lines.size(),
            collect
        );
        assertEquals("1a", lines.get(0).line(), collect);
        assertEquals("11", lines.get(13).line(), collect);
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
        Shape shape = Shape.size(Files.size(pathWithHeaders)).header(3, 2);

        List<NpLine> syncLines = new ArrayList<>();
        int partitionCount = 2; //Runtime.getRuntime().availableProcessors();
        Partitioning partitioning = new Partitioning(partitionCount, 16);

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        Partitioned<Path> pf1 = PartitionedPaths.create(pathWithHeaders, shape, partitioning);
        pf1.streams().streamers()
            .forEach(partitionStreamer ->
                partitionStreamer.lines()
                    .forEach(syncLines::add));
        pf1.close();
        assertContents(syncLines);

        Partitioned<Path> pf2 = PartitionedPaths.create(pathWithHeaders, shape, partitioning);

        List<NpLine> asyncLines = new ArrayList<>();

        pf2.streams().streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(streamer::lines, executorService))
            .map(future ->
                future.thenAccept(partitionedLineStream ->
                    partitionedLineStream.forEach(asyncLines::add)))
            .forEach(CompletableFuture::join);
        pf2.close();
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
        Shape shape = Shape.size(Files.size(pathWithHeaders)).utf8();

        List<NpLine> syncLines = new ArrayList<>();

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        int partitionCount = Runtime.getRuntime().availableProcessors();
        Partitioning partitioning = new Partitioning(partitionCount, 16);
        Partitioned<Path> pf1 = PartitionedPaths.create(pathWithHeaders, shape, partitioning, executorService);
        pf1.streams().streamers()
            .forEach(partitionStreamer ->
                partitionStreamer.lines()
                    .forEach(syncLines::add)
            );
        pf1.close();
        assertContents(syncLines);

        Partitioned<Path> pf2 = PartitionedPaths.create(pathWithHeaders, shape, partitioning, executorService);

        List<NpLine> asyncLines = new ArrayList<>();

        pf2.streams().streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(streamer::lines, executorService))
            .map(future ->
                future.thenAccept(partitionedLineStream ->
                    partitionedLineStream.forEach(asyncLines::add)))
            .forEach(CompletableFuture::join);
        pf2.close();
        assertContents(asyncLines);
    }
}
