package com.github.kjetilv.flopp.kernel;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.stream.Collectors.joining;

public final class FileBuilder {

    public static Path file(Path tmp, String base, int lineCount, int columnCount, Shape.Decor decor) {
        try {
            Path path = tmp.resolve(base + "-data.txt");
            Iterable<String> lines = lines(lineCount, columnCount, decor);
            Files.write(path, lines, WRITE, CREATE);
            return path;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create file with " + lineCount + " lines", e);
        }
    }

    private FileBuilder() {
    }

    private static Iterable<String> lines(int lineCount, int colCount, Shape.Decor decor) {
        int header = decor.header();
        int footer = decor.footer();
        AtomicLong countdown = new AtomicLong(lineCount + header + footer);

        return () -> new Iterator<>() {

            @Override
            public boolean hasNext() {
                return countdown.getAndDecrement() > 0;
            }

            @Override
            public String next() {
                long c = countdown.get();
                if (c - lineCount + 1 >= header) {
                    return "HEADER";
                }
                if (c < footer) {
                    return "FOOTER";
                }
                return LongStream.range(0, colCount)
                    .map(l -> c * c + l).mapToObj(Long::toString)
                    .collect(joining(";"));
            }
        };
    }
}
