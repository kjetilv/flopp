package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.files.PartitionedPaths;
import com.github.kjetilv.flopp.kernel.partitions.Partitioning;
import com.github.kjetilv.flopp.kernel.partitions.Partitionings;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

class VectorPartitionedTest {

    @Test
    void testWithHeader() throws IOException {
        Path pathWithHeaders = Files.write(
            Files.createTempFile(UUID.randomUUID().toString(), ".test"),
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
                
                011
                01a
                01aa
                01aaa
                02aaaaaa
                03bbbb
                04bbbbbbbb
                05ccccc
                06cccccccccc
                07dddddd
                08dddddddddddd
                09eeeeee
                010
                
                111
                111
                11a
                11aa
                11aaa
                12aaaaaa
                13bbbb
                14bbbbbbbb
                15ccccc
                16cccccccccc
                17dddddd
                18dddddddddddd
                19eeeeee
                10
                
                11
                """.split("\n"))
        );
        Shape shape = Shape.size(Files.size(pathWithHeaders), UTF_8).header(1);
        List<String> syncLines = new ArrayList<>();
        Partitioning partitioning = PARTITIONINGS.create(5, 32);
        try (
            Partitioned pf1 = PartitionedPaths.vectorPartitioned(pathWithHeaders, partitioning, shape)
        ) {
            pf1.streamers()
                .forEach(partitionStreamer ->
                    partitionStreamer.lines()
                        .map(lineSegment -> lineSegment.asString(UTF_8))
                        .forEach(add(syncLines)));
        }
        assertContents(syncLines);

        List<String> asyncLines = new ArrayList<>();

        try (
            Partitioned pf2 = PartitionedPaths.partitioned(pathWithHeaders, partitioning, shape);
            ExecutorService executorService = Executors.newFixedThreadPool(10)
        ) {
            pf2.streamers()
                .map(streamer ->
                    CompletableFuture.supplyAsync(streamer::lines, executorService))
                .map(future ->
                    future.thenAccept(partitionedLineStream ->
                        partitionedLineStream.map(lineSegment -> lineSegment.asString(UTF_8))
                            .forEach(add(asyncLines))))
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
                
                011
                01a
                01aa
                01aaa
                02aaaaaa
                03bbbb
                04bbbbbbbb
                05ccccc
                06cccccccccc
                07dddddd
                08dddddddddddd
                09eeeeee
                010
                
                111
                111
                11a
                11aa
                11aaa
                12aaaaaa
                13bbbb
                14bbbbbbbb
                15ccccc
                16cccccccccc
                17dddddd
                18dddddddddddd
                19eeeeee
                10
                
                11
                FOOTER1
                FOOTER2
                """.split("\n"))
        );
        Shape shape = Shape.size(Files.size(pathWithHeaders), UTF_8).headerFooter(3, 2);

        List<String> syncLines = new ArrayList<>();
        int partitionCount = 2; //Runtime.getRuntime().availableProcessors();
        Partitioning partitioning = PARTITIONINGS.create(partitionCount, 16);

        try (
            Partitioned pf1 = PartitionedPaths.partitioned(pathWithHeaders, partitioning, shape)
        ) {
            pf1.streamers()
                .forEach(partitionStreamer ->
                    partitionStreamer.lines()
                        .map(lineSegment -> lineSegment.asString(UTF_8))
                        .forEach(add(syncLines)));
        }
        assertContents(syncLines);

        List<String> asyncLines = new ArrayList<>();
        try (
            Partitioned pf2 = PartitionedPaths.partitioned(pathWithHeaders, partitioning, shape);
            ExecutorService executorService = Executors.newFixedThreadPool(10)
        ) {
            pf2.streamers()
                .map(streamer ->
                    CompletableFuture.supplyAsync(streamer::lines, executorService))
                .map(future ->
                    future.thenAccept(partitionedLineStream ->
                        partitionedLineStream.map(lineSegment -> lineSegment.asString(UTF_8))
                            .forEach(add(asyncLines))))
                .forEach(CompletableFuture::join);
        }
        assertContents(asyncLines);
    }

    @Disabled
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
                
                011
                01a
                01aa
                01aaa
                02aaaaaa
                03bbbb
                04bbbbbbbb
                05ccccc
                06cccccccccc
                07dddddd
                08dddddddddddd
                09eeeeee
                010
                
                111
                111
                11a
                11aa
                11aaa
                12aaaaaa
                13bbbb
                14bbbbbbbb
                15ccccc
                16cccccccccc
                17dddddd
                18dddddddddddd
                19eeeeee
                10
                
                11
                """.split("\n"))
        );
        Shape shape = Shape.size(Files.size(pathWithHeaders), UTF_8);

        List<String> syncLines = new ArrayList<>();

        Partitioning partitioning = PARTITIONINGS.create(4, 16);
        try (
            Partitioned pf1 = PartitionedPaths.partitioned(pathWithHeaders, partitioning, shape)
        ) {
            pf1.streamers()
                .forEach(partitionStreamer ->
                    partitionStreamer.lines()
                        .map(lineSegment -> lineSegment.asString(UTF_8))
                        .forEach(add(syncLines))
                );
        }
        assertContents(syncLines);

        List<String> asyncLines = new ArrayList<>();
        try (
            Partitioned pf2 = PartitionedPaths.partitioned(pathWithHeaders, partitioning, shape);
            ExecutorService executorService = Executors.newFixedThreadPool(10)
        ) {
            pf2.streamers()
                .map(streamer ->
                    CompletableFuture.supplyAsync(streamer::lines, executorService))
                .map(future ->
                    future.thenAccept(partitionedLineStream ->
                        partitionedLineStream.map(lineSegment -> lineSegment.asString(UTF_8))
                            .forEach(add(asyncLines))))
                .forEach(CompletableFuture::join);
        }
        assertContents(asyncLines);
    }

    private static final Partitionings PARTITIONINGS = Partitionings.BYTE_VECTOR;

    private static Consumer<String> add(List<String> syncLines) {
        return e -> {
            if (e.contains("\n")) {
                throw new IllegalStateException("Line has more than one line: `" + e + "`");
            }
            syncLines.add(e);
        };
    }

    private static void assertContents(List<String> lines) {
        String collect = lines.stream()
            .map(Object::toString)
            .collect(Collectors.joining("\n"));
        assertEquals(
            43, lines.size(),
            collect
        );
        assertEquals("1a", lines.getFirst(), collect);
        assertEquals("01a", lines.get(14), collect);
    }
}
