package com.github.kjetilv.flopp.kernel.util;

import com.github.kjetilv.flopp.kernel.Non;

import java.util.HashMap;
import java.util.Map;

public final class Maps {

    private Maps(){

    }

    private static final int MAX_POWER = 1 << Integer.SIZE - 2;

    public static <K, V> Map<K, V> ofSize(int size) {
        return new HashMap<>(capacity(size));
    }

    private static int capacity(int size) {
        if (Non.negative(size, "size") < 3) {
            return size + 1;
        }
        if (size < MAX_POWER) {
            return (int) (1.0f * size / 0.75f + 1.0f);
        }
        return Integer.MAX_VALUE;
    }
}
