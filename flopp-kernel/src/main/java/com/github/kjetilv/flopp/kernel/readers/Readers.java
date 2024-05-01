package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.CsvFormat;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class Readers {

    public static Reader create(Column<?>... columns) {
        return create(List.of(columns));
    }

    public static Reader create(Path file, CsvFormat format) {
        return create(firstLine(file), format);
    }

    public static Reader create(String header, CsvFormat format) {
        List<Column<String>> columns = discoverColumns(header, format);
        return readerFor(columnMap(columns));
    }

    @SuppressWarnings("unchecked")
    public static Reader create(List<? extends Column<?>> columns) {
        return readerFor(columnMap((List<Column<Object>>) columns));
    }

    private Readers() {
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

    private static <T> Reader readerFor(Map<String, Column<T>> columnMap) {
        return (splitter, values) -> {
            splitter.forEach(separatedLine -> {
                Map<String, Object> valueMap = new HashMap<>();
                values.accept(name ->
                    valueMap.computeIfAbsent(name, _ -> {
                        Column<?> column = columnMap.get(name.toLowerCase(Locale.ROOT));
                        return column.parser().parse(separatedLine.segment(column.colunmNo() - 1));
                    })
                );
            });
        };
    }

    private static String firstLine(Path file) {
        Optional<String> firstLine;
        try (BufferedReader bufferedReader = Files.newBufferedReader(file)) {
            firstLine = bufferedReader.lines()
                .findFirst();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read file: " + file, e);
        }
        return firstLine.orElseThrow(() ->
            new IllegalArgumentException("No line in " + file));
    }
}
