package com.github.kjetilv.flopp.kernel;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class Maps {

    public static <K, V, T> Map<K, V> map(
        Function<T, K> key, Function<T, V> value, T... list
    ) {
        return Arrays.stream(list)
            .collect(Collectors.toMap(key, value));
    }

    public static <K, V, T> Map<K, V> map(
        Function<T, K> key, Function<T, V> value, Collection<T> list
    ) {
        return list.stream()
            .collect(Collectors.toMap(key, value));
    }

    private Maps() {

    }
}
