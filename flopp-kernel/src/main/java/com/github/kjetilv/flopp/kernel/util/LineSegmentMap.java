package com.github.kjetilv.flopp.kernel.util;

import com.github.kjetilv.flopp.kernel.LineSegment;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface LineSegmentMap<T> {

    T get(LineSegment segment, Supplier<T> newEntry);

    Stream<T> values();

    default Map<String, T> toStringMap() {
        return toStringMap(Charset.defaultCharset());
    }

    default Map<String, T> toStringMap(Charset charset) {
        return toKeyMap(lineSegment -> lineSegment.asString(charset));
    }

    default <K> Map<K, T> toKeyMap(Function<LineSegment, K> toKey) {
        return Maps.mapKeys(toMap(), toKey);
    }

    Map<LineSegment, T> toMap();
}
