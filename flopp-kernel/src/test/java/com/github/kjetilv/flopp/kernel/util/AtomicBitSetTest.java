package com.github.kjetilv.flopp.kernel.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class AtomicBitSetTest {

    @Test
    void testBitSet(){
        AtomicBitSet set = new AtomicBitSet(35);
        assertThat(set.set(23)).isTrue();
        assertThat(set.set(23)).isFalse();
        assertThat(set.get(22)).isFalse();
        assertThat(set.get(23)).isTrue();
    }
}