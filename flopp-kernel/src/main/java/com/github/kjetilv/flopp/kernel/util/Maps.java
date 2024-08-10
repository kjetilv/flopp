package com.github.kjetilv.flopp.kernel.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public final class Maps {

    public static <K, V, T> Map<T, V> mapKeys(Map<K, V> map, Function<K, T> mapper) {
        return map.entrySet()
            .stream()
            .collect(Collectors.toMap(
                e ->
                    mapper.apply(e.getKey()),
                Map.Entry::getValue
            ));
    }

    public static <K, V> Map<K, V> ofSize(int size) {
        return new HashMap<>(mapCapacity(size));
    }

    public static int mapCapacity(int size) {
        if (Non.negative(size, "size") < 3) {
            return size + 1;
        }
        if (size < MAX_POWER) {
            return (int) (1.0f * size / 0.75f + 1.0f);
        }
        return Integer.MAX_VALUE;
    }

    @SafeVarargs
    static <K, V, T> Map<K, V> map(Function<T, K> key, Function<T, V> value, T... list) {
        return Arrays.stream(list)
            .collect(Collectors.toMap(key, value));
    }

    static <K, V, T> Map<K, V> map(Collection<T> list, Function<T, K> key, Function<T, V> value) {
        return list.stream()
            .collect(Collectors.toMap(key, value));
    }

    private Maps() {

    }

    private static final int MAX_POWER = 1 << Integer.SIZE - 2;
}
