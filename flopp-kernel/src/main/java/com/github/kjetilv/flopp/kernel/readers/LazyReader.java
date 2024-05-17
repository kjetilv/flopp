package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.util.Maps;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.IntStream;

final class LazyReader<T> implements Reader {

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

    private final Map<String, Column<T>> columnMap;

    private LazyReader(Map<String, Column<T>> columnMap) {
        this.columnMap = Objects.requireNonNull(columnMap, "columnMap");
    }

    @Override
    public void read(PartitionedSplitter splitter, Consumer<Columns> values) {
        splitter.forEach(separatedLine ->
            values.accept(columns(separatedLine)));
    }

    private Columns columns(SeparatedLine separatedLine) {
        Map<String, Object> valueMap = Maps.ofSize(columnMap.size());
        return name ->
            valueMap.computeIfAbsent(name, _ -> {
                Column<?> column = columnMap.get(name);
                int columnIndex = column.colunmNo() - 1;
                LineSegment segment = separatedLine.segment(columnIndex);
                return column.parser().parse(segment);
            });
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
