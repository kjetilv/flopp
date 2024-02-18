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

import static com.github.kjetilv.flopp.kernel.bits.BitwiseLineSplitter.Lines;
import static org.assertj.core.api.Assertions.assertThat;

class BitwiseLineSplitterTest {

    @Test
    void splitLine() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            new LineSplit(';'),
            adder(splits)
        );
        LineSegment lineSegment = LineSegment.of("foo123;bar;234;abcdef;3456");
        splitter.accept(lineSegment);
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
            new LineSplit(';'),
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
    void shortString() {
        assertSplit(
            "foo;bar;zot",
            "foo",
            "bar",
            "zot"
        );
    }

    @Test
    void shorterString() {
        assertSplit(
            "foo;bar",
            "foo",
            "bar"
        );
    }

    @Test
    void shorterString8() {
        assertSplit(
            "fooz;bar",
            "fooz",
            "bar"
        );
    }

    @Test
    void veryShorterString() {
        assertSplit(
            "a;b;c",
            "a",
            "b",
            "c"
        );
    }

    @Test
    void quoted() {
        assertSplit(
            "'foo 1';bar;234;'ab; cd;ef';'it is \\'aight';;234;',';'\\;'",
            "'foo 1'",
            "bar",
            "234",
            "'ab; cd;ef'",
            "'it is \\'aight'",
            "",
            "234",
            "','",
            "'\\;'"
        );
    }

    @Test
    void quotedLimitedButNotReally() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            new LineSplit(';', '\''),
            adder(splits)
        );
        splitter.accept(LineSegment.of(
            "'foo 1';bar;234;'ab; cd;ef';'it is \\'aight';;234;',';'\\;'"));
        assertThat(splits).containsExactly(
            "'foo 1'", "bar", "234", "'ab; cd;ef'", "'it is \\'aight'", "", "234", "','", "'\\;'");
    }

    @Test
    void quotedPickAll() {
        List<String> splits = new ArrayList<>();
        String input = "'foo 1';bar;234;'ab; cd;ef';'it is \\'aight';;234;',';'\\;'";
        String[] expected = {"'foo 1'", "bar", "234", "'ab; cd;ef'", "'it is \\'aight'", "", "234", "','", "'\\;'"};
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            new LineSplit(';', '\''),
            adder(splits)
        );
        splitter.accept(LineSegment.of(
            input));
        assertThat(splits).containsExactly(
            expected);
    }

    @Test
    void splitFile() throws IOException {
        List<String> splits = new ArrayList<>();
        Path path = Files.write(
            Files.createTempFile(UUID.randomUUID().toString(), ".txt"),
            List.of(
                """
                    def123;cba;234;abcdef;3456
                    abc234;foo;456;dfgfgh;1234
                    """
                    .split("\n")
            )
        );
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            new LineSplit(';'),
            line ->
                line.columnStream()
                    .forEach(splits::add)
        );
        try (Partitioned<Path> partititioned = Bitwise.partititioned(path, Partitioning.single())) {
            partititioned.streams().streamers()
                .forEach(streamer ->
                    streamer.lines()
                        .forEach(splitter));
        }
        assertThat(splits).containsAll(
            Stream.of(
                    "def123;cba;234;abcdef;3456".split(";"),
                    "abc234;foo;456;dfgfgh;1234".split(";")
                )
                .flatMap(Arrays::stream)
                .toList()
        );
    }

    private static void assertSplit(String input, String... expected) {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            new LineSplit(';', '\''),
            adder(splits));
        splitter.accept(LineSegment.of(input));
        assertThat(splits).containsExactly(expected);
    }

    private static Lines adder(List<String> splits) {
        return line ->
            line.columnStream()
                .forEach(splits::add);
    }
}
