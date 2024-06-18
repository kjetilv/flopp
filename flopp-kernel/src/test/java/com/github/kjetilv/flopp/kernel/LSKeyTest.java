package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class LSKeyTest {

    @Test
    void lsKeyTest() {
        LSKey foobar = lsKey("foobarzot faderullandei");
        assertThat(foobar).hasToString("foobarzot faderullandei");
    }

    @Test
    void lsKeySubTest() {
        LSKey foobar = lsKey("foobarzot faderullandei", 3, 10);
        assertThat(foobar).hasToString("barzot fad");
        assertThat(foobar).hasSameHashCodeAs(lsKey("barzot fad"));
    }

    @Test
    void lsKeyHashcode() {
        assertThat(lsKey("foobar")).doesNotHaveSameHashCodeAs(lsKey("zotzip"));
    }

    private static LSKey lsKey(String key) {
        return LSKey.create(LineSegments.of(key, UTF_8));
    }

    private static LSKey lsKey(String key, int offset, int length) {
        return LSKey.create(LineSegments.of(key, UTF_8).slice(offset, offset + length));
    }
}
