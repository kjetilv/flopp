package com.github.kjetilv.flopp.kernel.lc;

import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Shape;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class IndexingLineCounter implements LineCounter {

    private final Partitioning partitioning;

    private final Shape shape;

    private final int scanResolution;

    public IndexingLineCounter(Partitioning partitioning, Shape shape, int scanResolution) {
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.shape = Objects.requireNonNull(shape, "fileShape");
        this.scanResolution = scanResolution;
    }

    @Override
    public long count(Path path) {
        try (InputStream inputStream = bytes(path)) {
            return lineCount(inputStream);
        } catch (Exception e) {
            throw new IllegalStateException(STR."Could not count lines: \{path}", e);
        }
    }

    @Override
    public Lines scan(Path path) {
        if (shape.size() > 0) {
            try (InputStream inputStream = bytes(path)) {
                return lines(inputStream);
            } catch (Exception e) {
                throw new IllegalStateException(STR."Could not count lines: \{path}", e);
            }
        }
        throw new IllegalStateException(STR."Empty file: \{path}");
    }

    @Override
    public String toString() {
        return STR."\{
            getClass().getSimpleName()
            }[\{shape} / \{partitioning}]";
    }

    private ByteIndexEstimator lines(InputStream is) {
        byte[] buffer = new byte[BUFFER_SIZE];
        ByteIndexEstimator estimator = new ByteIndexEstimator(shape, partitioning, scanResolution);
        long offset = 0;
        while (true) {
            int b;
            try {
                b = is.read(buffer);
            } catch (Exception e) {
                throw new IllegalStateException(STR."Failed to read from \{is}", e);
            }
            if (b < 0) {
                return estimator;
            }
            for (int i = 0; i < b; i++) {
                if (buffer[i] == '\n') {
                    estimator.lineAt(offset + i + 1);
                }
            }
            offset += b;
        }
    }

    public static final int BUFFER_SIZE = 8192;

    private static InputStream bytes(Path path) {
        try {
            return new BufferedInputStream(
                Files.newInputStream(path, StandardOpenOption.READ),
                BUFFER_SIZE
            );
        } catch (Exception e) {
            throw new IllegalStateException(STR."Failed to read: \{path}", e);
        }
    }

    private static long lineCount(InputStream is) {
        byte[] buffer = new byte[BUFFER_SIZE];
        long c = 0;
        while (true) {
            int b;
            try {
                b = is.read(buffer);
            } catch (Exception e) {
                throw new IllegalStateException(STR."Failed to read from \{is}", e);
            }
            if (b < 0) {
                return c;
            }
            for (int i = 0; i < b; i++) {
                if (buffer[i] == '\n') {
                    c++;
                }
            }
        }
    }
}
