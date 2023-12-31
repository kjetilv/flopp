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
            throw new IllegalStateException("Could not count lines: " + path, e);
        }
    }

    @Override
    public Lines scan(Path path) {
        if (shape.size() > 0) {
            try (InputStream inputStream = bytes(path)) {
                return lines(inputStream);
            } catch (Exception e) {
                throw new IllegalStateException("Could not count lines: " + path, e);
            }
        }
        throw new IllegalStateException("Empty file: " + path);
    }

    private ByteIndexEstimator lines(InputStream is) {
        byte[] buffer = new byte[partitioning.bufferSize()];
        ByteIndexEstimator estimator = new ByteIndexEstimator(shape, partitioning, scanResolution);
        long offset = 0;
        while (true) {
            int b;
            try {
                b = is.read(buffer);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to read from " + is, e);
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

    private long lineCount(InputStream is) {
        byte[] buffer = new byte[partitioning.bufferSize()];
        long c = 0;
        while (true) {
            int b;
            try {
                b = is.read(buffer);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to read from " + is, e);
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

    private InputStream bytes(Path path) {
        try {
            return new BufferedInputStream(
                Files.newInputStream(path, StandardOpenOption.READ),
                partitioning.bufferSize()
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read: " + path, e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + shape + " / " + partitioning + "]";
    }
}
