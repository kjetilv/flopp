package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.Shape;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class BitwiseLineSplitterTest {

    @Test
    void splitLine() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            adder(splits)
        );
        splitter.accept(LineSegment.of("foo123;bar;234;abcdef;3456"));
        assertThat(splits).containsExactly(
            "foo123",
            "bar",
            "234",
            "abcdef",
            "3456"
        );
    }

    @Test
    void quoted() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            '\'',
            '\\',
            adder(splits)
        );
        splitter.accept(LineSegment.of(
            "'foo 1';bar;234;'ab; cd;ef';'it is \\'aight';;234;',';'\\;'"));
        assertThat(splits).containsExactly(
            "'foo 1'", "bar", "234", "'ab; cd;ef'", "'it is \\'aight'", "", "234", "','", "'\\;'");
    }

    @Test
    void quotedLimited() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            '\'',
            '\\',
            adder(splits),
            3
        );
        splitter.accept(LineSegment.of(
            "'foo 1';bar;234;'ab; cd;ef';'it is \\'aight';;234;',';'\\;'"));
        assertThat(splits).containsExactly(
            "'foo 1'", "bar", "234");
    }

    @Test
    void quotedPicky() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            '\'',
            '\\',
            adder(splits),
            new int[] {3, 4, 7}
        );
        splitter.accept(LineSegment.of(
            "'foo 1';bar;234;'ab; cd;ef';'it is \\'aight';;234;',';'\\;'"));
        assertThat(splits).containsExactly(
            "'ab; cd;ef'", "'it is \\'aight'", "','");
    }

    @Test
    void splitLinePicky() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            (segment, startIndex, endIndex) ->
                splits.add(LineSegment.of(segment, startIndex, endIndex).immutable().asString()),
            new int[] {1, 3, 4}
        );
        splitter.accept(LineSegment.of("foo123;bar;234;abcdef;3456"));
        assertThat(splits).containsExactly(
            "bar",
            "abcdef",
            "3456"
        );
    }

    @Test
    void simple() throws IOException {
        Instant now = Instant.now();
        Set<String> airlines = new HashSet<>();
        try (Stream<String> lines = Files.lines(PATH)) {
            lines.skip(1)
                .forEach(line -> {
                    String[] split = line.split(",");
                    airlines.add(split[1]);
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
        Path path = PATH;
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ',',
            (segment, startIndex, endIndex) -> {
                airlines.add(LineSegment.of(segment, startIndex, endIndex).asString());
            },
            new int[] {1}
        );
        try (
            Partitioned<Path> partititioned = Bitwise.partititioned(path, Shape.of(path).header(2));
            ForkJoinPool executor = new ForkJoinPool(partititioned.partitions().size());
        ) {
            partititioned.streams().streamers()
                .map(partitionStreamer -> {
                    return CompletableFuture.runAsync(
                        () ->
                            partitionStreamer.lines()
                                .forEach(splitter),
                        executor
                    );
                })
                .toList()
                .forEach(CompletableFuture::join);
        }
        System.out.println(airlines.size());
        System.out.println(airlines.stream().limit(10)
            .collect(Collectors.joining(", ")));
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    public static final Path PATH =
        Path.of("/Users/kjetilvalstadsve/Downloads/kaggle-flights/Combined_Flights_2021.csv");

    private static BitwisePartitioned.Action adder(List<String> splits) {
        return (segment, startIndex, endIndex) -> {
            String string = LineSegment.of(segment, startIndex, endIndex).immutable().asString();
            System.out.println(string);
            splits.add(string);
        };
    }
}
