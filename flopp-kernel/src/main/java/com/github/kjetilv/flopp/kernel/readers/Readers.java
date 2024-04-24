package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.CsvFormat;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

public final class Readers {

    public static Column column(String name, int columnNo) {
        return column(name, columnNo, null);
    }

    public static Column column(String name, int columnNo, Column.Parser parser) {
        return new Column(name, columnNo, parser);
    }

    public static Reader create(Column... columns) {
        return create(List.of(columns));
    }

    public static Reader create(Path file, CsvFormat format) {
        return create(firstLine(file), format);
    }

    public static Reader create(String header, CsvFormat format) {
        String[] headers = header.split(Character.toString(format.separator()));
        return create(IntStream.range(0, headers.length)
            .mapToObj(i ->
                new Column(headers[i], i + 1))
            .toList());
    }

    public static Reader create(List<Column> columns) {
        return (splitter, values) ->
            splitter.forEach(separatedLine -> {
                values.accept(Maps.map(Column::name, column ->
                        column.parse(separatedLine.segment(column.colunmNo() - 1)),
                    columns
                ));
            });
    }

    private Readers() {
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
