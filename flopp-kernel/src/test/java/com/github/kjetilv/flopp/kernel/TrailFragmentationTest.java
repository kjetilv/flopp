package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

class TrailFragmentationTest {

    @Test
    void fragment() {
        TrailFragmentation trailFragmentation = new TrailFragmentation(25, 9, 1.0, 60.0);
        TrailFragmentation.Result result = trailFragmentation.create(1_234_310, 10, 256);
        System.out.println(result);
    }

}
