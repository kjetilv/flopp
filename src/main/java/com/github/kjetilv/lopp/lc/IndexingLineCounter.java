package com.github.kjetilv.lopp.lc;

import com.github.kjetilv.lopp.FileShape;
import com.github.kjetilv.lopp.Partitioning;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class IndexingLineCounter implements LineCounter {

    private final Partitioning partitioning;

    private final FileShape fileShape;

    public IndexingLineCounter(Partitioning partitioning, FileShape fileShape) {
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.fileShape = Objects.requireNonNull(fileShape, "fileShape");
    }

    @Override
    public long count(Path path) {
        return scan(path).linesCount();
    }

    @Override
    public Lines scan(Path path) {
        long byteSize = fileShape.stats().fileSize();
        if (byteSize <= 0) {
            throw new IllegalStateException("Empty file: " + path);
        }
        try (InputStream inputStream = bytes(path)) {
            return lines(inputStream);
        } catch (Exception e) {
            throw new IllegalStateException("Could not count lines: " + path, e);
        }
    }

    private ByteIndexEstimator lines(InputStream is) {
        byte[] buffer = new byte[partitioning.bufferSize()];
        ByteIndexEstimator estimator = new ByteIndexEstimator(fileShape, partitioning);
        long offset = 0;
        while (true) {
            int b = 0;
            try {
                b = is.read(buffer);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to read from " + is, e);
            }
            if (b < 0) {
                return estimator;
            }
            for (int i = 0; i != b; i++) {
                if (buffer[i] == '\n') {
                    estimator.lineAt(offset + i + 1);
                }
            }
            offset += b;
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
        return getClass().getSimpleName() + "[" + fileShape + " / " + partitioning + "]";
    }
}
