package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BitsTest {

    @Test
    void countOne() {
        Bits.Counter counter = Bits.counter('\n');
        assertThat(counter.count(0x120A340A560AL)).isEqualTo(3);
    }

    @Test
    void findOne() {
        Bits.Finder finder = Bits.finder('\n');

        assertThat(finder.next(0x120A340A560AL)).isEqualTo(0);
        assertThat(finder.hasNext()).isTrue();
        assertThat(finder.next()).isEqualTo(2);
        assertThat(finder.hasNext()).isTrue();
        assertThat(finder.next()).isEqualTo(4);
        assertThat(finder.hasNext()).isFalse();
        assertThat(finder.next()).isEqualTo(8);
    }
}
