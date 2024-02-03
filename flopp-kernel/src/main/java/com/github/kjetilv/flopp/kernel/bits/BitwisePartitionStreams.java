package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public final class BitwisePartitionStreams implements PartitionedStreams {

    private final Path path;

    private final Shape shape;

    private final List<Partition> partitions;

    private final MemorySegmentSource memorySegmentSource;

    public BitwisePartitionStreams(Path path, Shape shape, List<Partition> partitions) {
        this.path = Objects.requireNonNull(path, "path");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.partitions = Objects.requireNonNull(partitions, "partitions");
        if (this.partitions.isEmpty()) {
            throw new IllegalArgumentException("No partitions receveid");
        }
        this.memorySegmentSource = new MemorySegmentSource(
            this.path,
            this.shape,
            Arena::ofAuto
        );
    }

    @Override
    public Stream<? extends PartitionStreamer> streamers() {
        return streamers(new LinkedList<>(partitions)).stream();
    }

    @Override
    public void close() {
        try {
            memorySegmentSource.close();
        } catch (Exception e) {
            throw new RuntimeException(STR."\{this} could not close", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{path}/\{shape}/ partitions:\{partitions.size()}]";
    }

    private LinkedList<BitwisePartitionStreamer> streamers(
        LinkedList<Partition> partitionsTail
    ) {
        if (partitionsTail.isEmpty()) {
            return new LinkedList<>();
        }
        Partition head = partitionsTail.removeFirst();
        LinkedList<BitwisePartitionStreamer> streamersTail = streamers(partitionsTail);
        BitwisePartitionStreamer nextStreamer = streamersTail.isEmpty()
            ? null
            : streamersTail.getFirst();
        BitwisePartitionStreamer streamer = new BitwisePartitionStreamer(
            head,
            shape,
            memorySegmentSource,
            nextStreamer
        );
        streamersTail.addFirst(streamer);
        return streamersTail;
    }
}
