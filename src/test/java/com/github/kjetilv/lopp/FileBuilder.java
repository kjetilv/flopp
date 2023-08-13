package com.github.kjetilv.lopp;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.stream.Collectors.joining;

final class FileBuilder {

    static Path file(Path tmp, String base, FileShape shape, int lineCount, int columnCount) {
        try {
            Path path = tmp.resolve(base + "-data.txt");
            Iterable<String> lines = lines(shape, lineCount, columnCount);
            Files.write(path, lines, WRITE, CREATE);
            return path;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create file with " + lineCount + " lines", e);
        }
    }

    private FileBuilder() {
    }

    private static Iterable<String> lines(FileShape fileShape, int lineCount, int colCount) {
        int header = fileShape.decor().header();
        int footer = fileShape.decor().footer();
        AtomicLong countdown = new AtomicLong(lineCount + header + footer);
        Iterable<String> lines = () -> new Iterator<>() {

            @Override
            public boolean hasNext() {
                return countdown.getAndDecrement() > 0;
            }

            @Override
            public String next() {
                long c = countdown.get();
                if (c - lineCount >= header) {
                    return "HEADER";
                }
                if (c < footer) {
                    return "FOOTER";
                }
                return LongStream.range(0, colCount)
                    .map(l -> c + l).mapToObj(Long::toString)
                    .collect(joining(";"));
            }
        };

        return lines;
    }
}
