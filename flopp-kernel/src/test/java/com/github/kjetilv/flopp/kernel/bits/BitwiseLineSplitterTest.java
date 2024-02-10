package com.github.kjetilv.flopp.kernel.bits;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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

    private static BitwisePartitioned.Action adder(List<String> splits) {
        return (segment, startIndex, endIndex) -> {
            splits.add(LineSegment.of(segment, startIndex, endIndex).immutable().asString());
        };
    }
}
