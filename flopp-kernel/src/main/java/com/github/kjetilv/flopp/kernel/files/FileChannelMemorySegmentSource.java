package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.MemorySegmentSource;
import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.io.IOException;
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
        Shape shape,
        Partition partition,
        FileChannel channel,
        Arena arena,
        int padding
    ) {
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partition = Objects.requireNonNull(partition, "partition");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.arena = Objects.requireNonNull(arena, "arena");
        this.padding = Non.negative(padding, "padding");
    }

    @Override
    public Segment get() {
        int remainingSize = Math.toIntExact(shape.size() - partition.offset());
        int wantedSize = partition.count() + padding;
        int logicalLength = Math.min(remainingSize, wantedSize);
        int physicalLength = Math.max(SPECIES_LENGTH, logicalLength);
        int shift = Math.toIntExact(physicalLength - logicalLength);
        try {
            return new Segment(memorySegment(physicalLength, shift), shift);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this}: Could not open with length \{logicalLength}", e);
        }
    }

    private MemorySegment memorySegment(long length, int shift) {
        try {
            return channel.map(
                READ_ONLY,
                partition.offset() - shift,
                length,
                arena
            ).asReadOnly();
        } catch (IOException e) {
            throw new IllegalStateException(STR."\{this} failed to open \{channel}", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{shape} \{partition}, padding=\{padding}]";
    }

    public static final int SPECIES_LENGTH = MemorySegmentSource.SPECIES.length();
}
