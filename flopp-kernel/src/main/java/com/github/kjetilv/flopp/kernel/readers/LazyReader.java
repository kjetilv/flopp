package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.util.Maps;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

final class LazyReader<T> implements Reader, Reader.Columns {

    @SuppressWarnings("unchecked")
    static Reader create(List<? extends Column<?>> columns) {
        return readerFor(columnMap((List<Column<Object>>) columns));
    }

    static Reader create(String header, CsvFormat format) {
        List<Column<String>> columns = discoverColumns(header, format);
        return readerFor(columnMap(columns));
    }

    static <T> Reader readerFor(Map<String, Column<T>> columnMap) {
        return new LazyReader<>(columnMap);
    }

    private final String[] columnKeys;

    private final Column<?>[] columns;

    private final Map<String, Supplier<Object>> valueMap;

    private final int length;

    private LazyReader(Map<String, Column<T>> columnMap) {
        this(
            columnMap.keySet().toArray(String[]::new),
            columnMap.values().toArray(Column[]::new)
        );
    }

    private LazyReader(String[] columnKeys, Column<?>[] columns) {
        this.columnKeys = columnKeys;
        this.columns = columns;
        this.length = this.columns.length;
        this.valueMap = Maps.ofSize(this.length);
    }

    @Override
    public void read(PartitionedSplitter splitter, Consumer<Columns> values) {
        splitter.forEach(separatedLine ->
            values.accept(
                columns(separatedLine)));
    }

    @Override
    public Reader copy() {
        return new LazyReader<>(columnKeys, columns);
    }

    @Override
    public Object get(String name) {
        return valueMap.get(name).get();
    }

    private Columns columns(SeparatedLine separatedLine) {
        for (int i = 0; i < length; i++) {
            valueMap.put(columnKeys[i], parse(separatedLine, i));
        }
        return this;
    }

    private Supplier<Object> parse(SeparatedLine separatedLine, int index) {
        return () -> {
            Column<?> column = columns[index];
            Column.Parser<?> parser = column.parser();
            LineSegment segment = separatedLine.segment(column.colunmNo() - 1);
            return parser.parse(segment);
        };
    }

    private static List<Column<String>> discoverColumns(String header, CsvFormat format) {
        String[] headers = format.split(header);
        return IntStream.range(0, headers.length)
            .mapToObj(i ->
                Column.ofString(headers[i], i + 1))
            .toList();
    }

    private static <T> Map<String, Column<T>> columnMap(List<Column<T>> columns) {
        Map<String, Column<T>> map = Maps.ofSize(columns.size());
        for (Column<T> column : columns) {
            map.put(column.name(), column);
        }
        return Map.copyOf(map);
    }
}
