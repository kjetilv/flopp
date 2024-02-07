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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class BitwiseLineSplitterTest {

    @Test
    void splitLine() {
        List<LineSegment> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            (segment, startIndex, endIndex) ->
                splits.add(LineSegment.of(segment, startIndex, endIndex).immutable())
        );
        splitter.accept(LineSegment.of("foo123;bar;234;abcdef;3456"));
        assertThat(splits).containsExactly(
            LineSegment.of("foo123"),
            LineSegment.of("bar"),
            LineSegment.of("234"),
            LineSegment.of("abcdef"),
            LineSegment.of("3456")
        );
    }

    @Disabled
    @Test
    void quoted() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            '\'',
            (segment, startIndex, endIndex) ->
                splits.add(LineSegment.of(segment, startIndex, endIndex).immutable().asString())
        );
        splitter.accept(LineSegment.of("'foo 123';bar;234;'ab, cd, ef';'it is 3456';;',';"));
        assertThat(splits).containsExactly(
            "foo 123",
            "bar",
            "234",
            "ab, cd, ef",
            "it is 3456",
            "",
            ",",
            ""
        );
    }

    @Test
    void splitLinePicky() {
        List<LineSegment> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            (segment, startIndex, endIndex) ->
                splits.add(LineSegment.of(segment, startIndex, endIndex).immutable()),
            1, 3, 4
        );
        splitter.accept(LineSegment.of("foo123;bar;234;abcdef;3456"));
        assertThat(splits).containsExactly(
            LineSegment.of("bar"),
            LineSegment.of("abcdef"),
            LineSegment.of("3456")
        );
    }

    @Test
    void simple() throws IOException {
        Instant now = Instant.now();
        Set<String> manu = new HashSet<>();
        try (Stream<String> lines = Files.lines(PATH)) {
            lines
                .skip(2)
                .limit(5_000_000)
                .forEach(line ->
                {
                    String[] split = line.split(",");
                    if (split.length > 2 && !split[2].isBlank()) {
                        manu.add(split[2]);
                    }
                });
        }
        System.out.println(manu.size());
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    @Test
    void faster() throws IOException {
        Instant now = Instant.now();
        Set<String> manu = new HashSet<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ',',
            (segment, startIndex, endIndex) -> {
                String lineSegment = LineSegment.of(segment, startIndex, endIndex).asString();
                if (!lineSegment.isBlank()) {
                    manu.add(lineSegment);
                }
            },
            2
        );

        Path path = PATH;
        try (Stream<String> lines = Files.lines(path)) {
            lines.skip(2).limit(5_000_000)
                .map(LineSegment::of)
                .forEach(splitter);
        }
        System.out.println(manu.size());
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    @Test
    void fasterStill() {
        Instant now = Instant.now();
        Set<String> manu = new HashSet<>();
        Path path = PATH;
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ',',
            (segment, startIndex, endIndex) -> {
                String lineSegment = LineSegment.of(segment, startIndex, endIndex).asString();
                if (!lineSegment.isBlank()) {
                    manu.add(lineSegment);
                }
            },
            2
        );
        try (Partitioned<Path> partititioned = Bitwise.partititioned(path, Shape.of(path).header(2))) {
            partititioned.streams().streamers()
                .forEach(streamer ->
                    streamer.lines()
                        .forEach(splitter));
        }
        System.out.println(manu.size());
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    public static final Path PATH =
        Path.of("/Users/kjetilvalstadsve/Downloads/kaggle-opensky/aircraftDatabase.csv");
}
