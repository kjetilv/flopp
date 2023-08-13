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

class PartitionedFileTest {

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
        FileShape fileShape = FileShape.base().header(1);

        List<NPLine> syncLines = new ArrayList<>();

        int partitionCount = Runtime.getRuntime()
            .availableProcessors();
        Partitioning partitioning = new Partitioning(partitionCount, 16, 1);
        PartitionedFile pf1 = PartitionedFile.create(pathWithHeaders, fileShape, partitioning);
        pf1.streamers()
            .forEach(partitionStreamer ->
                partitionStreamer.lines()
                    .forEach(syncLines::add));
        pf1.close();
        assertContents(syncLines);

        PartitionedFile pf2 = PartitionedFile.create(pathWithHeaders, fileShape, partitioning);

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        List<NPLine> asyncLines = new ArrayList<>();

        pf2.streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(streamer::lines, executorService))
            .map(future ->
                future.thenAccept(partitionedLineStream ->
                    partitionedLineStream.forEach(asyncLines::add)))
            .forEach(CompletableFuture::join);
        pf2.close();
        assertContents(asyncLines);
    }

    private static void assertContents(List<NPLine> lines) {
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
        FileShape fileShape = FileShape.base().header(3, 2);

        List<NPLine> syncLines = new ArrayList<>();
        int partitionCount = Runtime.getRuntime().availableProcessors();
        Partitioning partitioning = new Partitioning(partitionCount, 16, 1);

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        PartitionedFile pf1 = PartitionedFile.create(pathWithHeaders, fileShape, partitioning);
        pf1.streamers()
            .forEach(partitionStreamer ->
                partitionStreamer.lines()
                    .forEach(e ->
                        syncLines.add(e)));
        pf1.close();
        assertContents(syncLines);

        PartitionedFile pf2 = PartitionedFile.create(pathWithHeaders, fileShape, partitioning);

        List<NPLine> asyncLines = new ArrayList<>();

        pf2.streamers()
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
        FileShape fileShape = FileShape.base().utf8();

        List<NPLine> syncLines = new ArrayList<>();

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        int partitionCount = Runtime.getRuntime()
            .availableProcessors();
        Partitioning partitioning = new Partitioning(partitionCount, 16, 1);
        PartitionedFile pf1 = PartitionedFile.create(pathWithHeaders, fileShape, partitioning, executorService);
        pf1.streamers()
            .forEach(partitionStreamer ->
                partitionStreamer.lines()
                    .forEach(syncLines::add)
            );
        pf1.close();
        assertContents(syncLines);

        PartitionedFile pf2 = PartitionedFile.create(pathWithHeaders, fileShape, partitioning, executorService);

        List<NPLine> asyncLines = new ArrayList<>();

        pf2.streamers()
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
