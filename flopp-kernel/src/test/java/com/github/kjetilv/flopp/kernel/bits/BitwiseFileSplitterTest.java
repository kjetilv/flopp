package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.Shape;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Disabled
class BitwiseFileSplitterTest {

    @Test
    void simple() throws IOException {
        Instant now = Instant.now();
        Set<String> airlines = new HashSet<>();
        try (Stream<Path> list = Files.list(PATH)) {
            list.filter(file -> file.getFileName().toString().endsWith(".csv"))
                .parallel()
                .forEach(file -> {
                    try (Stream<String> lines = Files.lines(file)) {
                        lines.skip(1)
                            .forEach(line -> {
                                String[] split = line.split(",");
                                airlines.add(split[1]);
                            });
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        }
        System.out.println(airlines.size());
        System.out.println(airlines.stream().limit(10)
            .collect(Collectors.joining(", ")));
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    @Test
    void faster() throws IOException {
        Instant now = Instant.now();
        Set<String> airlines = new HashSet<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ',',
            (segment, startIndex, endIndex) -> {
                airlines.add(LineSegment.of(segment, startIndex, endIndex).asString());
            },
            new int[] {1}
        );

        try (Stream<String> lines = Files.lines(PATH)) {
            lines.skip(1)
                .map(LineSegment::of)
                .forEach(splitter);
        }
        System.out.println(airlines.size());
        System.out.println(airlines.stream().limit(10)
            .collect(Collectors.joining(", ")));
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    @Test
    void fasterStill() {
        Instant now = Instant.now();
        Set<String> airlines = new HashSet<>();
        Path path = PATH;
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ',',
            (segment, startIndex, endIndex) -> {
                airlines.add(LineSegment.of(segment, startIndex, endIndex).asString());
            },
            new int[] {1}
        );
        try (Partitioned<Path> partititioned = Bitwise.partititioned(path, Shape.of(path).header(2))) {
            partititioned.streams().streamers()
                .forEach(streamer ->
                    streamer.lines()
                        .forEach(splitter));
        }
        System.out.println(airlines.size());
        System.out.println(airlines.stream().limit(10)
            .collect(Collectors.joining(", ")));
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    @Test
    void fasterStillParallel() {
        Instant now = Instant.now();
        Set<String> airlines = new HashSet<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ',',
            (segment, startIndex, endIndex) -> {
                airlines.add(LineSegment.of(segment, startIndex, endIndex).asString());
            },
            new int[] {1}
        );
        try (
            Stream<Path> list = Files.list(PATH);
            ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()
        ) {
            list.filter(file -> file.getFileName().toString().endsWith(".csv"))
                .flatMap(file ->
                    Bitwise.partititioned(file, Shape.of(file).header(2)).streams().streamers()
                        .map(partitionStreamer -> CompletableFuture.runAsync(
                            () ->
                                partitionStreamer.lines()
                                    .forEach(splitter),
                            executor
                        )))
                .toList()
                .forEach(CompletableFuture::join);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(airlines.size());
        System.out.println(airlines.stream().limit(10)
            .collect(Collectors.joining(", ")));
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    public static final Path PATH = Path.of(System.getProperty("csv.dir"));
}
