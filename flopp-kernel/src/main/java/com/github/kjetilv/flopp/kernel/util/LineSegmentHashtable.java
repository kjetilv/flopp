package com.github.kjetilv.flopp.kernel.util;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.bits.BitwiseTraverser;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
public final class LineSegmentHashtable<T> implements LineSegmentMap<T> {

    private final int limit;

    private final TableEntry<?>[] table;

    private final BitwiseTraverser.Reusable reusable;

    private int size;

    public LineSegmentHashtable(int limit) {
        this(limit, null);
    }

    public LineSegmentHashtable(int limit, BitwiseTraverser.Reusable reusable) {
        this.limit = Non.negativeOrZero(limit, "limit");
        this.table = new TableEntry<?>[this.limit];
        this.reusable = reusable == null ? BitwiseTraverser.create() : reusable;
    }

    @Override
    public T get(LineSegment segment) {
        return getOrCreate(segment, null).value();
    }

    public T get(LineSegment segment, Supplier<T> newEntry) {
        return getOrCreate(segment, newEntry).value();
    }

    @Override
    public T put(LineSegment segment, T value) {
        TableEntry<?> existing = store(new TableEntry<>(segment, reusable.toHashCode(segment), value));
        return existing == null ? null : (T) existing.value();
    }

    @Override
    public Stream<T> values() {
        return (Stream<T>) tableEntries().map(TableEntry::value);
    }

    @Override
    public void forEach(BiConsumer<LineSegment, T> consumer) {
        tableEntries().forEach(tableEntry ->
            consumer.accept(
                tableEntry.segment(),
                (T) tableEntry.value()
            ));
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Map<String, T> toStringMap(Charset charset) {
        if (charset == null || charset == CHARSET) {
            return tableEntries().collect(Collectors.toMap(
                TableEntry::segmentString,
                toValue()
            ));
        }
        return tableEntries().collect(Collectors.toMap(
            tableEntry -> tableEntry.segment().asString(charset),
            toValue()
        ));
    }

    @Override
    public String toStringSorted() {
        return toString(tableEntries()
            .sorted(
                Comparator.comparing(TableEntry::segmentString)));
    }

    @Override
    public <K> Map<K, T> toKeyMap(Function<LineSegment, K> toKey) {
        return tableEntries().collect(Collectors.toMap(
            tableEntry -> toKey.apply(tableEntry.segment()),
            tableEntry -> (T) tableEntry.value()
        ));
    }

    @Override
    public Map<LineSegment, T> toMap() {
        return tableEntries().collect(Collectors.toMap(
            TableEntry::segment,
            toValue()
        ));
    }

    @Override
    public T merge(LineSegment segment, T value, BiFunction<T, T, T> merger) {
        return (T) merge(
            new TableEntry<>(
                segment,
                reusable.toHashCode(segment),
                value
            ),
            merger
        ).value();
    }

    @Override
    public LineSegmentMap<T> merge(LineSegmentMap<T> incoming, BiFunction<T, T, T> merger) {
        ((LineSegmentHashtable<T>) incoming).tableEntries()
            .forEach(tableEntry ->
                merge(tableEntry, merger));
        return this;
    }

    @Override
    public String toString() {
        return toString(tableEntries());
    }

    public String toString(boolean sorted) {
        return toString(sorted
            ? tableEntries().sorted()
            : tableEntries());
    }

    private TableEntry<?> merge(TableEntry<?> tableEntry, BiFunction<T, T, T> merger) {
        LineSegment segment = tableEntry.segment();
        TableEntry<T> local = (TableEntry<T>) tableEntry(segment);
        if (local == null) {
            return store(tableEntry);
        }
        return store(local.merge(merger.apply(
            local.value(),
            (T) tableEntry.value()
        )));
    }

    @SuppressWarnings("unchecked")
    private TableEntry<T> getOrCreate(LineSegment segment, Supplier<T> newEntry) {
        int hash = reusable.toHashCode(segment);
        int initialPos = indexOf(hash);
        int slotPos = initialPos;
        while (true) {
            TableEntry<?> existing = table[slotPos];
            if (existing == null) {
                if (newEntry == null) {
                    return (TableEntry<T>) TableEntry.NULL;
                }
                T value = newEntry.get();
                if (value == null) {
                    return (TableEntry<T>) TableEntry.NULL;
                }
                TableEntry<T> newTableEntry =
                    new TableEntry<>(segment.hashedWith(hash), hash, value);
                table[slotPos] = newTableEntry;
                size++;
                return newTableEntry;
            }
            if (existing.matches(segment, hash)) {
                return (TableEntry<T>) existing;
            }
            slotPos = indexOf(slotPos + 1);
            if (slotPos == initialPos) {
                throw new IllegalStateException(this + " is full");
            }
        }
    }

    private TableEntry<?> store(TableEntry<?> tableEntry) {
        LineSegment segment = tableEntry.segment();
        int hash = tableEntry.hash();
        int initialPos = indexOf(hash);
        int slotPos = initialPos;
        while (true) {
            TableEntry<?> existing = table[slotPos];
            boolean newEntry = existing == null;
            if (newEntry) {
                size++;
            }
            if (newEntry || existing.matches(segment, hash)) {
                table[slotPos] = tableEntry;
                return existing;
            }
            slotPos = indexOf(slotPos + 1);
            if (slotPos == initialPos) {
                throw new IllegalStateException("The table is full: " + tableEntry);
            }
        }
    }

    private TableEntry<?> tableEntry(LineSegment segment) {
        int hash = reusable.toHashCode(segment);
        int initialPos = indexOf(hash);
        int slotPos = initialPos;
        while (true) {
            TableEntry<?> existing = table[slotPos];
            if (existing == null) {
                return null;
            }
            if (existing.matches(segment, hash)) {
                return existing;
            }
            slotPos = indexOf(slotPos + 1);
            if (slotPos == initialPos) {
                throw new IllegalStateException("The table is full: " + segment);
            }
        }
    }

    private Stream<TableEntry<?>> tableEntries() {
        return Arrays.stream(table)
            .filter(Objects::nonNull);
    }

    private int indexOf(int hash) {
        return hash & limit - 1;
    }

    private static final Charset CHARSET = Charset.defaultCharset();

    private static <T> Function<TableEntry<?>, T> toValue() {
        return tableEntry -> (T) tableEntry.value();
    }

    private static String toString(Stream<TableEntry<?>> tableEntries) {
        Stream<String> entries = tableEntries.flatMap(e -> Stream.of(
            ", ",
            e.segmentString(),
            "=",
            e.value().toString()
        ));
        return Stream.of(
            Stream.of("{"),
            entries.skip(1),
            Stream.of("}")
        ).flatMap(Function.identity())
            .collect(Collectors.joining());
    }
}
