package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

public interface Reader {

    static Column column(String name, int columnNo) {
        return column(name, columnNo, null);
    }

    static Column column(String name, int columnNo, Function<String, Object> parser) {
        return new Column(name, columnNo, parser);
    }

    static Reader of(Path file, CsvFormat format) {
        return of(firstLine(file), format);
    }

    static Reader of(String header, CsvFormat format) {
        String[] headers = header.split(Character.toString(format.separator()));
        return of(IntStream.range(0, headers.length)
            .mapToObj(i ->
                new Column(
                    headers[i],
                    i + 1,
                    s -> s
                ))
            .toList());
    }

    static Reader of(Column... columns) {
        return of(List.of(columns));
    }

    static Reader of(List<Column> columns) {
        return (splitter, values) ->
            splitter.forEach(separatedLine ->
                values.accept(Maps.map(
                    Column::name,
                    column -> value(separatedLine, column),
                    columns
                )));
    }

    void read(PartitionedSplitter splitter, Consumer<Map<String, Object>> values);

    private static Object value(
        SeparatedLine separatedLine,
        Column column
    ) {
        return column.parse(separatedLine.column(column.colunmNo() - 1));
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

    record Column(String name, int colunmNo, Function<String, Object> parser) {

        public Column {
            Non.negativeOrZero(colunmNo, "Columns are 1-indexed, first column is 1");
        }

        Object parse(String string) {
            return string == null || parser == null
                ? string
                : parser.apply(string);
        }
    }
}
