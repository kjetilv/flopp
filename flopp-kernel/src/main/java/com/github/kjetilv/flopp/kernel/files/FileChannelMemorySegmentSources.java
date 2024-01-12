package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.MemorySegmentSource;
import com.github.kjetilv.flopp.kernel.MemorySegmentSources;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public class FileChannelMemorySegmentSources implements MemorySegmentSources {

    private final Path path;

    private final Shape shape;

    public FileChannelMemorySegmentSources(Path path) {
        this(path, null);
    }

    public FileChannelMemorySegmentSources(Path path, Shape shape) {
        this(path, shape, null);
    }

    public FileChannelMemorySegmentSources(Path path, Shape shape, Arena arena) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = shape == null ? Shape.of(path) : shape;
    }

    @Override
    public MemorySegmentSource source(Partition partition) {
        FileChannel channel = channel();
        Arena arena = Arena.ofAuto();
        MemorySegment ms = memorySegment(partition, channel, arena);
        return new MemorySegmentSource() {

            @Override
            public MemorySegment get() {
                return ms;
            }

            @Override
            public void close() {
                try {
                    channel.close();
                } catch (Exception e) {
                    throw new IllegalStateException(STR."\{this} failed to close \{channel}", e);
                } finally {
                    arena.close();
                }
            }
        };
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{path}]";
    }

    private FileChannel channel() {
        try {
            return FileChannel.open(path, StandardOpenOption.READ);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to open channel: \{path}", e);
        }
    }

    private MemorySegment memorySegment(Partition partition, FileChannel channel, Arena arena) {
        try {
            return channel.map(READ_ONLY, partition.offset(), this.shape.size(), arena).asReadOnly();
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this}: Could not open", e);
        }
    }
}
