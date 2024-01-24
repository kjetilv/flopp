package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.LineSegments;
import com.github.kjetilv.flopp.kernel.files.PartitionedPaths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SegmentedPartitionedTest {

    @Test
    void testWithSimple(@TempDir Path tempDir) throws IOException {
        Path pathWithHeaders = tempDir.resolve(STR."\{UUID.randomUUID()}.test");
        List<String> lines = Arrays.asList("""
            1
            2a
                        
            3bb
            4c
            d
                        
            5e
            6ff
            """.split("\n"));
        Files.write(
            pathWithHeaders,
            lines
        );
        Shape shape = Shape.size(Files.size(pathWithHeaders)).longestLine(10);

        List<String> syncLines = new ArrayList<>();

        int partitionCount = 1; //Runtime.getRuntime().availableProcessors();
        Partitioning partitioning = new Partitioning(partitionCount, 16);
        Partitioned<Path> pf1 = PartitionedPaths.create(pathWithHeaders, shape, partitioning);
        pf1.streams().vectorStreamers()
            .forEach(partitionStreamer ->
                partitionStreamer.memorySegments()
                    .map(LineSegments::toString)
                    .forEach(e -> {
                        assertThat(e).doesNotContain("\n");
                        syncLines.add(e);
                    }));
        pf1.close();
        assertThat(syncLines).containsExactlyElementsOf(lines);
    }

    @Test
    void testWithSimpleEdge(@TempDir Path tempDir) throws IOException {
        Path pathWithHeaders = tempDir.resolve(STR."\{UUID.randomUUID()}.test");
        List<String> lines = Arrays.asList("""
            12345678
            2a
            2a
            2a
            2a
            2a
            2a
            """.split("\n"));
        Files.write(
            pathWithHeaders,
            lines
        );
        Shape shape = Shape.size(Files.size(pathWithHeaders)).longestLine(10);

        List<String> syncLines = new ArrayList<>();

        int partitionCount = 1; //Runtime.getRuntime().availableProcessors();
        Partitioning partitioning = Partitioning.longAligned(partitionCount);
        Partitioned<Path> pf1 = PartitionedPaths.create(pathWithHeaders, shape, partitioning);
        pf1.streams().vectorStreamers()
            .forEach(partitionStreamer ->
                partitionStreamer.memorySegments()
                    .map(LineSegments::toString)
                    .forEach(e -> {
                        assertThat(e).doesNotContain("\n");
                        syncLines.add(e);
                    }));
        pf1.close();
        assertThat(syncLines).containsExactlyElementsOf(lines);
    }

    @Test
    void testWithHeader(@TempDir Path tempDir) throws IOException {
        Path pathWithHeaders = tempDir.resolve(STR."\{UUID.randomUUID()}.test");
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
        Shape shape = Shape.size(Files.size(pathWithHeaders)).header(1).longestLine(10);

        List<String> syncLines = new ArrayList<>();

        int partitionCount = 3; //Runtime.getRuntime().availableProcessors();
        Partitioning partitioning = Partitioning.longAligned(partitionCount);
        Partitioned<Path> pf1 = PartitionedPaths.create(pathWithHeaders, shape, partitioning);
        pf1.streams().vectorStreamers()
            .forEach(partitionStreamer ->
                partitionStreamer.memorySegments()
                    .map(LineSegments::toString)
                    .forEach(e -> {
                        assertThat(e).doesNotContain("\n");
                        syncLines.add(e);
                    }));
        pf1.close();
        assertContents(syncLines);

        Partitioned<Path> pf2 = PartitionedPaths.create(pathWithHeaders, shape, partitioning);

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        List<String> asyncLines = new ArrayList<>();

        pf2.streams().vectorStreamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(streamer::memorySegments, executorService))
            .map(future ->
                future.thenAccept(partitionedLineStream ->
                    partitionedLineStream.map(LineSegments::toString)
                        .forEach(asyncLines::add)))
            .forEach(CompletableFuture::join);
        pf2.close();
        assertContents(asyncLines);
    }

    @Test
    void testWithHeaders(@TempDir Path tempDir) throws IOException {
        Path pathWithHeaders = tempDir.resolve(STR."\{UUID.randomUUID().toString()}.test");
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

        List<String> syncLines = new ArrayList<>();
        Partitioning partitioning = Partitioning.longAligned(2);

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        Partitioned<Path> pf1 = PartitionedPaths.create(pathWithHeaders, shape, partitioning);
        pf1.streams().vectorStreamers()
            .forEach(partitionStreamer ->
                partitionStreamer.memorySegments()
                    .map(LineSegments::toString)
                    .forEach(syncLines::add));
        pf1.close();
        assertContents(syncLines);

        Partitioned<Path> pf2 = PartitionedPaths.create(pathWithHeaders, shape, partitioning);

        List<String> asyncLines = new ArrayList<>();

        pf2.streams().vectorStreamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(streamer::memorySegments, executorService))
            .map(future ->
                future.thenAccept(partitionedLineStream ->
                    partitionedLineStream
                        .map(LineSegments::toString)
                        .forEach(asyncLines::add)))
            .forEach(CompletableFuture::join);
        pf2.close();
        assertContents(asyncLines);
    }

    @Test
    void testWithoutHeaders(@TempDir Path tempDir) throws IOException {
        Path pathWithHeaders = tempDir.resolve(STR."\{UUID.randomUUID().toString()}.test");
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

        List<String> syncLines = new ArrayList<>();

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        Partitioning partitioning = Partitioning.longAligned(5);
        Partitioned<Path> pf1 = PartitionedPaths.create(pathWithHeaders, shape, partitioning, executorService);
        pf1.streams().vectorStreamers()
            .forEach(partitionStreamer ->
                partitionStreamer.memorySegments()
                    .map(LineSegments::toString)
                    .forEach(syncLines::add)
            );
        pf1.close();
        assertContents(syncLines);

        Partitioned<Path> pf2 = PartitionedPaths.create(pathWithHeaders, shape, partitioning, executorService);

        List<String> asyncLines = new ArrayList<>();

        pf2.streams().vectorStreamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(streamer::memorySegments, executorService))
            .map(future ->
                future.thenAccept(partitionedLineStream ->
                    partitionedLineStream
                        .map(LineSegments::toString)
                        .forEach(asyncLines::add)))
            .forEach(CompletableFuture::join);
        pf2.close();
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
