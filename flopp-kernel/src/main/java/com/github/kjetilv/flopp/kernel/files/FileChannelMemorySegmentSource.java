package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.MemorySegmentSource;
import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.util.Objects;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

final class FileChannelMemorySegmentSource implements MemorySegmentSource {

    private final Shape shape;

    private final Partition partition;

    private final FileChannel channel;

    private final Arena arena;

    private final int padding;

    FileChannelMemorySegmentSource(
        Partition partition,
        Shape shape,
        FileChannel channel,
        Arena arena,
        int padding
    ) {
        this.partition = Objects.requireNonNull(partition, "partition");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.arena = Objects.requireNonNull(arena, "arena");
        this.padding = Non.negative(padding, "padding");
    }

    @Override
    public Segment get() {
        long logicalLength = logicalLength();
        long physicalLength = Math.max(SPECIES_LENGTH, logicalLength);
        int shift = Math.toIntExact(physicalLength - logicalLength);
        MemorySegment memorySegment;
        try {
            memorySegment = channel.map(
                READ_ONLY,
                partition.offset() - shift,
                physicalLength,
                arena
            ).asReadOnly();
        } catch (Exception e) {
            throw new IllegalStateException(
                STR."\{this}: Could not open with length \{physicalLength}, shift \{shift}", e);
        }
        return new Segment(memorySegment, shift);
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{shape} \{partition}, padding=\{padding}]";
    }

    private long logicalLength() {
        long ideallength = shape.limitsLineLength()
            ? partition.count() + shape.stats().longestLine() + 1
            : partition.count() + padding;
        long lengthAvailable = shape.size() - partition.offset();
        return Math.min(ideallength, lengthAvailable);
    }

    public static final int SPECIES_LENGTH = MemorySegmentSource.SPECIES.length();
}
