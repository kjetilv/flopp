package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

class TailResultShardsTest {

    @Test
    void fragment() {
        TailShards tailShards = new TailShards(25, 9, 60.0, 1.0);
        Partitions tailResult = tailShards.create(1_234_310, 10, 256);
        System.out.println(tailResult);
    }
}
