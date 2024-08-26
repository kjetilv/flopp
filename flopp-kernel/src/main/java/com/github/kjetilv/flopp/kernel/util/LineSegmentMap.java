package com.github.kjetilv.flopp.kernel.util;

import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings({"UnusedReturnValue", "unused"})
public interface LineSegmentMap<T> {

    default Stream<T> values() {
        return entries().map(Map.Entry::getValue);
    }

    default Stream<Map.Entry<String, T>> stringEntries() {
        return entries().map(entry ->
            Map.entry(entry.getKey().asString(), entry.getValue()));
    }

    default void forEach(BiConsumer<LineSegment, T> consumer) {
        entries().forEach(entry -> consumer.accept(entry.getKey(), entry.getValue()));
    }

    default Map<String, T> toStringMap() {
        return toStringMap(Charset.defaultCharset());
    }

    default Map<String, T> toStringMap(Charset charset) {
        return toKeyMap(lineSegment -> lineSegment.asString(charset));
    }

    default String toStringSorted() {
        return str(stringEntries().sorted(Comparator.comparing(Map.Entry::getKey)));
    }

    default <K> Map<K, T> toKeyMap(Function<LineSegment, K> toKey) {
        return entries().collect(Collectors.toMap(
            entry -> toKey.apply(entry.getKey()),
            Map.Entry::getValue
        ));
    }

    default Map<LineSegment, T> toMap() {
        return entries().collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue
        ));
    }

    T get(LineSegment segment);

    T get(LineSegment segment, Supplier<T> newEntry);

    T put(LineSegment segment, T value);

    Stream<Map.Entry<LineSegment, T>> entries();

    T merge(LineSegment segment, T value, BiFunction<T, T, T> merger);

    LineSegmentMap<T> merge(LineSegmentMap<T> incoming, BiFunction<T, T, T> merger);

    private String str(Stream<Map.Entry<String, T>> entries) {
        Stream<String> strings = entries.flatMap(e -> Stream.of(
            ", ",
            e.getKey(),
            "=",
            e.getValue().toString()
        ));
        return Stream.of(
                Stream.of("{"),
                strings.skip(1),
                Stream.of("}")
            ).flatMap(Function.identity())
            .collect(Collectors.joining());
    }
}
