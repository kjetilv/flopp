package com.github.kjetilv.flopp.kernel.segments;

import com.github.kjetilv.flopp.kernel.util.Non;

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
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
final class LineSegmentHashtable<T> implements LineSegmentMap<T> {

    private final int limit;

    private final TableEntry<?>[] table;

    private final LineSegmentTraverser.Reusable reusable;

    LineSegmentHashtable(int limit, LineSegmentTraverser.Reusable reusable) {
        this.limit = Non.negativeOrZero(limit, "limit");
        this.table = new TableEntry<?>[this.limit];
        this.reusable = reusable == null ? LineSegmentTraverser.create() : reusable;
    }

    private LineSegmentHashtable(int limit, TableEntry<?>... table) {
        this.limit = limit;
        this.table = table;
        this.reusable = LineSegmentTraverser.create();
    }

    @Override
    public Stream<T> values() {
        return (Stream<T>) tableEntries().map(TableEntry::value);
    }

    @Override
    public Stream<Map.Entry<String, T>> stringEntries() {
        return tableEntries().map(tableEntry ->
            Map.entry(tableEntry.segmentString(), (T) tableEntry.value()));
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
        return str(tableEntries()
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
    public LineSegmentMap<T> freeze() {
        LineSegment segment = LineSegments.segmentOfSize(lengths().sum());
        int[] lengths = lengths().toArray();
        int[] indexes = activeIndexes();
        TableEntry<T>[] frozenEntries = new TableEntry[table.length];
        int offset = 0;
        for (int i = 0; i < indexes.length; i++) {
            frozenEntries[indexes[i]] = entry(indexes[i]).freezeTo(segment, offset);
            offset += lengths[i];
        }
        return new LineSegmentHashtable<>(limit, frozenEntries);
    }

    @Override
    public T get(LineSegment segment) {
        int hash = reusable.toHashCode(segment);
        int initialPos = indexOf(hash);
        int slotPos = initialPos;
        while (true) {
            TableEntry<T> existing = entry(slotPos);
            if (existing == null) {
                return null;
            }
            if (existing.matches(segment, hash)) {
                return existing.value();
            }
            slotPos = indexOf(slotPos + 1);
            if (slotPos == initialPos) {
                throw new IllegalStateException(this + " is full");
            }
        }
    }

    @Override
    public T get(LineSegment segment, Supplier<T> valueSupplier) {
        int hash = reusable.toHashCode(segment);
        int initialPos = indexOf(hash);
        int slotPos = initialPos;
        while (true) {
            TableEntry<T> existing = entry(slotPos);
            if (existing == null) {
                T value = valueSupplier.get();
                table[slotPos] = new TableEntry<>(segment.alignedHashedWith(hash), hash, value);
                return value;
            }
            if (existing.matches(segment, hash)) {
                return existing.value();
            }
            slotPos = indexOf(slotPos + 1);
            if (slotPos == initialPos) {
                throw new IllegalStateException(this + " is full");
            }
        }
    }

    @Override
    public T put(LineSegment segment, T value) {
        TableEntry<?> existing =
            store(new TableEntry<>(segment, reusable.toHashCode(segment), value));
        return existing == null ? null : (T) existing.value();
    }

    @Override
    public Stream<Map.Entry<LineSegment, T>> entries() {
        return tableEntries().map(tableEntry ->
            Map.entry(tableEntry.segment(), (T) tableEntry.value()));
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

    private IntStream lengths() {
        return Arrays.stream(table)
            .filter(Objects::nonNull)
            .mapToInt(entry ->
                Math.toIntExact(entry.segment().length()));
    }

    private TableEntry<?> merge(TableEntry<?> newEntry, BiFunction<T, T, T> merger) {
        TableEntry<T> local = (TableEntry<T>) tableEntry(newEntry.segment());
        return store(local == null
            ? newEntry
            : local.merge(merger.apply(local.value(), (T) newEntry.value())));
    }

    private TableEntry<?> store(TableEntry<?> tableEntry) {
        LineSegment segment = tableEntry.segment();
        int hash = tableEntry.hash();
        int initialPos = indexOf(hash);
        int slotPos = initialPos;
        while (true) {
            TableEntry<?> existing = entry(slotPos);
            if (existing == null || existing.matches(segment, hash)) {
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

    private int[] activeIndexes() {
        return IntStream.range(0, table.length)
            .filter(this::isSet)
            .toArray();
    }

    private TableEntry<T> entry(int i) {
        return (TableEntry<T>) table[i];
    }

    private boolean isSet(int i) {
        return table[i] != null;
    }

    private static final Charset CHARSET = Charset.defaultCharset();

    private static <T> Function<TableEntry<?>, T> toValue() {
        return tableEntry ->
            (T) tableEntry.value();
    }

    private static String str(Stream<TableEntry<?>> tableEntries) {
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

    @Override
    public String toString() {
        return str(tableEntries());
    }

    public String toString(boolean sorted) {
        return str(sorted
            ? tableEntries().sorted()
            : tableEntries());
    }
}
