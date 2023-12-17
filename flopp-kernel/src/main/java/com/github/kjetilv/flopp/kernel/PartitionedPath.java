package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.files.FileChannelTransfers;
import com.github.kjetilv.flopp.kernel.files.FileTempTargets;
import com.github.kjetilv.flopp.kernel.files.MemoryMappedByteArrayLinesWriter;

import java.nio.file.Path;

@SuppressWarnings("unused")
public interface PartitionedPath extends Partitioned<Path> {

    default PartitionedProcessor processor(Path path, Partitioning partitioning) {
        return processor(
            new FileTempTargets(path),
            new FileChannelTransfers(path),
            PartitionedPaths::sizeOf,
            (p, charset) ->
                new MemoryMappedByteArrayLinesWriter(p, partitioning.bufferSize(), charset));
    }
}
