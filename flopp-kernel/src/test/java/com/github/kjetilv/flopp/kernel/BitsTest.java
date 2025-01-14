package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BitsTest {

    @Test
    void countOne() {
        Bits.Counter counter = counter('\n');
        assertThat(counter.count(0x120A340A560AL)).isEqualTo(3);
    }

    @Test
    void findNone() {
        Bits.Finder finder = finder('\n');

        assertThat(finder.next(0x5678120234045607L)).isEqualTo(8);
        assertThat(finder.hasNext()).isFalse();
    }

    @Test
    void findOne() {
        Bits.Finder finder = finder('\n');

        assertThat(finder.next(0x120A340A560AL)).isEqualTo(0);
        assertThat(finder.hasNext()).isTrue();
        assertThat(finder.next()).isEqualTo(2);
        assertThat(finder.hasNext()).isTrue();
        assertThat(finder.next()).isEqualTo(4);
        assertThat(finder.hasNext()).isFalse();
        assertThat(finder.next()).isEqualTo(8);
    }

    @Test
    void bin() {
        String bin = Bits.bin(49L, true);
        assertThat(bin).isEqualTo(
            "00000000.00000000.00000000.00000000.00000000.00000000.00000000.00110001"
        );
    }

    @Test
    void hxD() {
        String bin = Bits.hxD(49L);
        assertThat(bin).isEqualTo(
            "0x00.00.00.00.00.00.00.31"
        );
    }

    @Test
    void findOnEdge() {
        Bits.Finder finder = finder('"');

        assertThat(finder.next(0x2322002323230000L)).isEqualTo(6);
        assertThat(finder.next()).isEqualTo(8);
        assertThat(finder.hasNext()).isFalse();
    }

    @Test
    void findInside() {
        Bits.Finder finder = finder('"');

        assertThat(finder.next(0x0022242200000000L)).isEqualTo(4);
        assertThat(finder.next()).isEqualTo(6);
        assertThat(finder.hasNext()).isFalse();
    }

    @Test
    void findInsideEdgy() {
        Bits.Finder finder = finder('"');

        assertThat(finder.next(0x0022232200000000L)).isEqualTo(4);
        assertThat(finder.next()).isEqualTo(6);
        assertThat(finder.hasNext()).isFalse();
    }

    @Test
    void setBytes() {
        long l = 0x0A320A330A340A35L;
        byte[] bytes = Bits.toBytes(l);
        byte[] alsoBytes = new byte[8];
        Bits.transferLimitedDataTo(l, 0, 8, alsoBytes);
        assertThat(alsoBytes).containsExactly(bytes);
    }

    private static Bits.Counter counter(char c) {
        return Bits.counter(c);
    }

    private static Bits.Finder finder(char c) {
        return Bits.finder(c);
    }
}
