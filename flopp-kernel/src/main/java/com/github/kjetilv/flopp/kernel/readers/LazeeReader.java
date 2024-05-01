package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

final class LazeeReader<T> implements Reader {

    static <T> Reader readerFor(Map<String, Column<T>> columnMap) {
        return new LazeeReader<>(columnMap);
    }

    static Reader create(String header, CsvFormat format) {
        List<Column<String>> columns = discoverColumns(header, format);
        return readerFor(columnMap(columns));
    }

    @SuppressWarnings("unchecked")
    static Reader create(List<? extends Column<?>> columns) {
        return readerFor(columnMap((List<Column<Object>>) columns));
    }

    private final Map<String, Column<T>> columnMap;

    private LazeeReader(Map<String, Column<T>> columnMap) {
        this.columnMap = columnMap;
    }

    @Override
    public void read(PartitionedSplitter splitter, Consumer<Columns> values) {
        splitter.forEach(separatedLine -> {
            Map<String, Object> valueMap = new HashMap<>();
            values.accept(name ->
                valueMap.computeIfAbsent(name, _ -> {
                    Column<?> column = columnMap.get(name.toLowerCase(Locale.ROOT));
                    return column.parser().parse(separatedLine.segment(column.colunmNo() - 1));
                })
            );
        });
    }

    private static List<Column<String>> discoverColumns(String header, CsvFormat format) {
        String[] headers = header.split(Character.toString(format.separator()));
        return IntStream.range(0, headers.length)
            .mapToObj(i ->
                Column.ofString(headers[i], i + 1))
            .toList();
    }

    private static <T> Map<String, Column<T>> columnMap(List<Column<T>> columns) {
        return columns.stream()
            .collect(Collectors.toMap(
                col ->
                    col.name().toLowerCase(Locale.ROOT),
                Function.identity(),
                (c, _) -> c,
                LinkedHashMap::new
            ));
    }
}
