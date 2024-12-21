package com.github.kjetilv.flopp.kernel.partitions;

import com.github.kjetilv.flopp.kernel.Partitions;
import com.github.kjetilv.flopp.kernel.TailShards;
import org.junit.jupiter.api.Test;

class TailResultShardsTest {

    @Test
    void fragment() {
        TailShards tailShards = new TailShards(25, 1, 2, 3);
        Partitions tailResult = tailShards.create(1_234_310, 10, 256);
        System.out.println(tailResult);
    }
}
