package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class FileChannelMemorySegmentSources implements MemorySegmentSources {

    private final Path path;

    private final Shape shape;

    private final int padding;

    private final Arena arena;

    private final FileChannel channel;

    public FileChannelMemorySegmentSources(Path path) {
        this(path, null, null, 0);
    }

    public FileChannelMemorySegmentSources(Path path, Shape shape, Arena arena, int padding) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = shape == null ? Shape.of(path) : shape;
        try {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
        } catch (IOException e) {
            throw new IllegalArgumentException(STR."Failed to channel \{path}", e);
        }
        this.arena = Objects.requireNonNull(arena, "arena");
        this.padding = Non.negative(padding, "padding");
    }

    @Override
    public MemorySegmentSource source(Partition partition) {
        try {
            return new FileChannelMemorySegmentSource(shape, partition, this.channel, this.arena, this.padding);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to open channel: \{path}", e);
        }
    }

    @Override
    public void close() {
        try {
            arena.close();
        } catch (UnsupportedOperationException ignore) {
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to close \{arena}", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{path}]";
    }
}
