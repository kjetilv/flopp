package com.github.kjetilv.flopp.kernel.util;

import com.github.kjetilv.flopp.kernel.LineSegment;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

@SuppressWarnings({"UnusedReturnValue", "unused"})
public interface LineSegmentMap<T> {

    T get(LineSegment segment);

    T get(LineSegment segment, Supplier<T> newEntry);

    T put(LineSegment segment, T value);

    Stream<T> values();

    void forEach(BiConsumer<LineSegment, T> consumer);

    default Map<String, T> toStringMap() {
        return toStringMap(Charset.defaultCharset());
    }

    default Map<String, T> toStringMap(Charset charset) {
        return toKeyMap(lineSegment -> lineSegment.asString(charset));
    }

    String toStringSorted();

    <K> Map<K, T> toKeyMap(Function<LineSegment, K> toKey);

    Map<LineSegment, T> toMap();

    T merge(LineSegment segment, T value, BiFunction<T, T, T> merger);

    LineSegmentMap<T> merge(LineSegmentMap<T> incoming, BiFunction<T, T, T> merger);
}
