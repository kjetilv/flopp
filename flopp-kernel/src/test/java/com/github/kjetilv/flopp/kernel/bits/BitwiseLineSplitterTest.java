package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.Partitioning;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static com.github.kjetilv.flopp.kernel.bits.BitwiseLineSplitter.*;
import static org.assertj.core.api.Assertions.assertThat;

class BitwiseLineSplitterTest {

    @Test
    void splitLine() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            DEFAULT_QUOTE,
            DEFAULT_ESC,
            adder(splits),
            0,
            null
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
    void splitLineTwice() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            DEFAULT_QUOTE,
            DEFAULT_ESC,
            adder(splits),
            0,
            null
        );
        splitter.accept(LineSegment.of("foo123;bar;234;abcdef;3456"));
        assertThat(splits).containsExactly(
            "foo123",
            "bar",
            "234",
            "abcdef",
            "3456"
        );
        splits.clear();
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
            adder(splits),
            0,
            null
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
            3,
            null
        );
        splitter.accept(LineSegment.of(
            "'foo 1';bar;234;'ab; cd;ef';'it is \\'aight';;234;',';'\\;'"));
        assertThat(splits).containsExactly(
            "'foo 1'", "bar", "234");
    }

    @Test
    void quotedLimitedButNotReally() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            '\'',
            '\\',
            adder(splits),
            100,
            null
        );
        splitter.accept(LineSegment.of(
            "'foo 1';bar;234;'ab; cd;ef';'it is \\'aight';;234;',';'\\;'"));
        assertThat(splits).containsExactly(
            "'foo 1'", "bar", "234", "'ab; cd;ef'", "'it is \\'aight'", "", "234", "','", "'\\;'");
    }

    @Test
    void quotedPicky() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            '\'',
            '\\',
            adder(splits),
            0,
            new int[] {3, 4, 7}
        );
        splitter.accept(LineSegment.of(
            "'foo 1';bar;234;'ab; cd;ef';'it is \\'aight';;234;',';'\\;'"));
        assertThat(splits).containsExactly(
            "'ab; cd;ef'", "'it is \\'aight'", "','");
    }

    @Test
    void quotedPickAll() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            '\'',
            '\\',
            adder(splits),
            0,
            new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
        );
        splitter.accept(LineSegment.of(
            "'foo 1';bar;234;'ab; cd;ef';'it is \\'aight';;234;',';'\\;'"));
        assertThat(splits).containsExactly(
            "'foo 1'", "bar", "234", "'ab; cd;ef'", "'it is \\'aight'", "", "234", "','", "'\\;'");
    }

    @Test
    void splitLinePicky() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            DEFAULT_QUOTE,
            DEFAULT_ESC,
            (_, segment, startIndex, endIndex) ->
                splits.add(LineSegment.of(segment, startIndex, endIndex).immutable().asString()),
            0,
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
    void splitFile() throws IOException {
        List<String> splits = new ArrayList<>();
        Path path = Files.write(
            Files.createTempFile(UUID.randomUUID().toString(), ".txt"),
            List.of(
                "foo123;bar;234;abcdef;3456",
                "bar234;foo;456;dfgfgh;1234"
            )
        );
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            ';',
            DEFAULT_QUOTE,
            DEFAULT_ESC,
            (origin, segment, startIndex, endIndex) ->
                splits.add(
                    LineSegment.ofRange(segment, startIndex, endIndex).asString()
                ),
            0,
            null
        );
        try (Partitioned<Path> partititioned = Bitwise.partititioned(path, Partitioning.single())) {
            partititioned.streams().streamers()
                .forEach(streamer ->
                    streamer.lines()
                        .forEach(splitter));
        }
        assertThat(splits).containsAll(
            Stream.of(
                    "foo123;bar;234;abcdef;3456".split(";"),
                    "bar234;foo;456;dfgfgh;1234".split(";")
                )
                .flatMap(Arrays::stream)
                .toList()
        );
    }

    private static Action adder(List<String> splits) {
        return (_, segment, startIndex, endIndex) ->
            splits.add(LineSegment.of(segment, startIndex, endIndex).immutable().asString());
    }
}
