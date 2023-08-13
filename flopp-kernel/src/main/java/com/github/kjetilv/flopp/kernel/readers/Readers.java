package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.CsvFormat;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public final class Readers {

    public static Reader create(Column... columns) {
        return create(List.of(columns));
    }

    public static Reader create(List<Column> columns) {
        return LazyReader.create(columns);
    }

    public static Reader create(Path file, CsvFormat format) {
        return LazyReader.create(firstLine(file), format);
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
