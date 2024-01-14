package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.MemorySegmentSource;
import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;
import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

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
        long length = Math.min(shape.size() - partition.offset(), partition.count() + padding);
        long paddedLength = Math.max(SPECIES.length(), length);
        int shift = Math.toIntExact(paddedLength - length);
        try {
            return new Segment(getMemorySegment(shift, paddedLength), shift);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this}: Could not open with length \{length}", e);
        }
    }

    private MemorySegment getMemorySegment(int shift, long paddedLength) throws IOException {
        MemorySegment segment = channel.map(
            READ_ONLY,
            partition.offset() - shift,
            paddedLength,
            arena
        ).asReadOnly();
        return segment;
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to close \{channel}", e);
        }
    }

    private static final VectorSpecies<Byte> SPECIES =
        VectorShape.preferredShape().withLanes(ByteVector.SPECIES_PREFERRED.elementType());
}
