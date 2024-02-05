package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bitwise;
import com.github.kjetilv.flopp.kernel.bits.LineSegment;
import org.junit.jupiter.api.Disabled;
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

@Disabled
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
        Shape shape = Shape.size(Files.size(pathWithHeaders)).header(1).longestLine(10);

        List<String> syncLines = new ArrayList<>();

        int partitionCount = Runtime.getRuntime()
            .availableProcessors();
        Partitioning partitioning = new Partitioning(partitionCount, 16);
        Partitioned<Path> pf1 = Bitwise.partititioned(pathWithHeaders, partitioning, shape);
        pf1.streams().streamers()
            .forEach(partitionStreamer ->
                partitionStreamer.lines()
                    .map(LineSegment::asString)
                    .forEach(syncLines::add));
        pf1.close();
        assertContents(syncLines);

        Partitioned<Path> pf2 = Bitwise.partititioned(pathWithHeaders, partitioning, shape);

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        List<String> asyncLines = new ArrayList<>();

        pf2.streams().streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(streamer::lines, executorService))
            .map(future ->
                future.thenAccept(partitionedLineStream ->
                    partitionedLineStream.map(LineSegment::asString).forEach(asyncLines::add)))
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
        Shape shape = Shape.size(Files.size(pathWithHeaders)).headerFooter(3, 2);

        List<String> syncLines = new ArrayList<>();
        int partitionCount = 2; //Runtime.getRuntime().availableProcessors();
        Partitioning partitioning = new Partitioning(partitionCount, 16);

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        Partitioned<Path> pf1 = Bitwise.partititioned(pathWithHeaders, partitioning, shape);
        pf1.streams().streamers()
            .forEach(partitionStreamer ->
                partitionStreamer.lines()
                    .map(LineSegment::asString)
                    .forEach(syncLines::add));
        pf1.close();
        assertContents(syncLines);

        Partitioned<Path> pf2 = Bitwise.partititioned(pathWithHeaders, partitioning, shape);

        List<String> asyncLines = new ArrayList<>();

        pf2.streams().streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(streamer::lines, executorService))
            .map(future ->
                future.thenAccept(partitionedLineStream ->
                    partitionedLineStream.map(LineSegment::asString).forEach(asyncLines::add)))
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

        List<String> syncLines = new ArrayList<>();

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        int partitionCount = Runtime.getRuntime().availableProcessors();
        Partitioning partitioning = new Partitioning(partitionCount, 16);
        Partitioned<Path> pf1 = Bitwise.partititioned(pathWithHeaders,  partitioning, shape);
        pf1.streams().streamers()
            .forEach(partitionStreamer ->
                partitionStreamer.lines()
                    .map(LineSegment::asString)
                    .forEach(syncLines::add)
            );
        pf1.close();
        assertContents(syncLines);

        Partitioned<Path> pf2 = Bitwise.partititioned(pathWithHeaders, partitioning, shape);

        List<String> asyncLines = new ArrayList<>();

        pf2.streams().streamers()
            .map(streamer ->
                CompletableFuture.supplyAsync(streamer::lines, executorService))
            .map(future ->
                future.thenAccept(partitionedLineStream ->
                    partitionedLineStream.map(LineSegment::asString).forEach(asyncLines::add)))
            .forEach(CompletableFuture::join);
        pf2.close();
        assertContents(asyncLines);
    }
}
