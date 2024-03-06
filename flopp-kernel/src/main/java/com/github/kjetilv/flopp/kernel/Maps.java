package com.github.kjetilv.flopp.kernel;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class Maps {

    public static <K, V, T> Map<K, V> map(
        Collection<T> list,
        Function<T, K> key,
        Function<T, V> value
    ) {
        return list.stream()
            .collect(Collectors.toMap(key, value));
    }

    private Maps() {

    }
}
