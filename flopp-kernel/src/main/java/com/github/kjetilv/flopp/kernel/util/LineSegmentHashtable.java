package com.github.kjetilv.flopp.kernel.util;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.bits.BitwiseTraverser;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
public final class LineSegmentHashtable<T> implements LineSegmentMap<T> {

    private final int size;

    private final Record<?>[] table;

    private final BitwiseTraverser.Reusable reusable;

    public LineSegmentHashtable(int size) {
        this.size = Non.negativeOrZero(size, "size");
        this.table = new Record<?>[this.size];
        this.reusable = BitwiseTraverser.create();
    }

    @SuppressWarnings("unchecked")
    public T get(LineSegment segment, Supplier<T> newEntry) {
        int hash = reusable.reset(segment).toHashCode();
        int initialPos = index(hash);
        int slotPos = initialPos;
        while (true) {
            Record<?> existing = table[slotPos];
            if (existing == null) {
                T value = newEntry.get();
                table[slotPos] = new Record<>(segment.hashedWith(hash), hash, value);
                return value;
            }
            if (existing.matches(segment, hash)) {
                return (T) existing.value();
            }
            slotPos = index(slotPos + 1);
            if (slotPos == initialPos) {
                throw new IllegalStateException("Go away, the table is full: " + newEntry.get());
            }
        }
    }

    @Override
    public Stream<T> values() {
        return (Stream<T>) entries().map(Record::value);
    }

    @Override
    public Map<String, T> toStringMap(Charset charset) {
        return entries()
            .collect(Collectors.toMap(
                record -> record.segment().asString(charset),
                record -> (T) record.value
            ));
    }

    @Override
    public <K> Map<K, T> toKeyMap(Function<LineSegment, K> toKey) {
        return entries()
            .collect(Collectors.toMap(
                record -> toKey.apply(record.segment()),
                record -> (T) record.value()
            ));
    }

    @Override
    public Map<LineSegment, T> toMap() {
        return entries()
            .collect(Collectors.toMap(
                Record::segment,
                record -> (T) record.value
            ));
    }

    private Stream<Record<?>> entries() {
        return Arrays.stream(table)
            .filter(Objects::nonNull);
    }

    private int index(int hash) {
        return hash & size - 1;
    }

    private record Record<T>(LineSegment segment, int hash, T value) {

        private boolean matches(LineSegment segment, int hash) {
            return hash == this.hash && this.segment.matches(segment);
        }
    }
}
