package com.github.kjetilv.flopp.kernel.lc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class SimpleLineCounter implements LineCounter {

    @Override
    public long count(Path path) {
        try (
            Stream<String> lines = Files.lines(path)
        ) {
            return lines.count();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to count lines in " + path, e);
        }
    }
}
