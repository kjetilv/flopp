package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.files.FileChannelTransfers;
import com.github.kjetilv.flopp.kernel.files.FileTempTargets;
import com.github.kjetilv.flopp.kernel.files.MemoryMappedByteArrayLinesWriter;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;

public class BitwisePartitioned {

    private final Path path;

    private final Partitioning partitioning;

    private final Shape shape;

    private final List<Partition> partitions;

    public BitwisePartitioned(
        Path path,
        Partitioning partitioning,
        Shape shape
    ) {
        this.path = path;
        this.partitioning = partitioning;
        this.shape = shape;
        this.partitions = partitioning.of(shape.size());
    }

    public <P> BitwisePartitionProcessor processor(Path target) {
        return new BitwisePartitionProcessor(
            mapper(),
            partitions,
            shape.charset(),
            this::writer,
            tempTargets(path),
            transfers(target)
        );
    }

    public BitwisePartitionedMapper mapper() {
        return new BitwisePartitionedMapper(streamers());
    }

    public BitwisePartitionStreamers streamers() {
        return new BitwisePartitionStreamers(path, shape, partitions);
    }

    private MemoryMappedByteArrayLinesWriter writer(Path target, Charset charset) {
        return new MemoryMappedByteArrayLinesWriter(target, partitioning.bufferSize(), charset);
    }

    private static Transfers<Path> transfers(Path target) {
        return new FileChannelTransfers(target);
    }

    private static TempTargets<Path> tempTargets(Path source) {
        return new FileTempTargets(source);
    }
}
