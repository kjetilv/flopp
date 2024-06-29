package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class LineSegmentKeyTest {

    @Test
    void lsKeyTest() {
        LineSegmentKey foobar = lsKey("foobarzot faderullandei");
        assertThat(foobar).hasToString("foobarzot faderullandei");
    }

    @Test
    void lsKeySubTest() {
        LineSegmentKey foobar = lsKey("foobarzot faderullandei", 3, 10);
        assertThat(foobar).hasToString("barzot fad");
        LineSegmentKey barzotFad = lsKey("barzot fad");
        assertThat(foobar).hasSameHashCodeAs(barzotFad);
    }

    @Test
    void lsKeyHashcode() {
        assertThat(lsKey("foobar")).doesNotHaveSameHashCodeAs(lsKey("zotzip"));
    }

    private static LineSegmentKey lsKey(String key) {
        return LineSegmentKey.create(LineSegments.of(key, UTF_8));
    }

    private static LineSegmentKey lsKey(String key, int offset, int length) {
        return LineSegmentKey.create(LineSegments.of(key, UTF_8).slice(offset, offset + length));
    }
}
