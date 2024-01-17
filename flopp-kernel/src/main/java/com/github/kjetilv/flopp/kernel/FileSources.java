package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.files.FileChannelByteSources;
import com.github.kjetilv.flopp.kernel.files.FileChannelMemorySegmentSources;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public final class FileSources implements Sources {

    private final Path path;

    private final Shape shape;

    private final int padding;

    private final AtomicReference<FileChannelMemorySegmentSources> bytesSources = new AtomicReference<>();

    private final AtomicReference<FileChannelByteSources> memorySegmentSources = new AtomicReference<>();

    public FileSources(Path path, Shape shape) {
        this(path, shape, 0);
    }

    public FileSources(Path path, Shape shape, int padding) {
        this.path = Objects.requireNonNull(path, "path");
        if (!Files.isRegularFile(path)) {
            throw new IllegalStateException(STR."Not a valid path: \{path}");
        }
        if (!Files.isReadable(path)) {
            throw new IllegalStateException(STR."Not a readable path: \{path}");
        }
        this.shape = shape == null ? Shape.of(this.path) : shape;
        this.padding = Non.negative(padding, "padding");
        if (!this.shape.limitsLineLength() && padding == 0) {
            throw new IllegalStateException(STR."\{shape} does not limit line length, so padding must be positive");
        }
    }

    @Override
    public MemorySegmentSources memorySegmentSources() {
        return bytesSources.updateAndGet(existing -> existing == null
            ? new FileChannelMemorySegmentSources(path, shape, padding)
            : existing);
    }

    @Override
    public ByteSources byteSources() {
        return memorySegmentSources.updateAndGet(existing -> existing == null
            ? new FileChannelByteSources(path, shape.size(), padding)
            : existing);
    }

    @Override
    public void close() {
        Stream.of(bytesSources, memorySegmentSources)
            .map(AtomicReference::get)
            .filter(Objects::nonNull)
            .forEach(closeable -> {
                try {
                    closeable.close();
                } catch (IOException e) {
                    throw new IllegalStateException(STR."\{this} failed to close \{closeable}", e);
                }
            });
    }
}
