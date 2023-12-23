package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.ByteSource;
import com.github.kjetilv.flopp.kernel.ByteSources;
import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.Partition;

import java.nio.file.Path;

final class FileChannelSources extends AbstractFileChanneling implements ByteSources {

    private final long size;

    FileChannelSources(Path path, long size) {
        super(path, false);
        this.size = Non.negative(size, "size");
    }

    @Override
    public ByteSource source(Partition partition) {
        return new FileChannelSource(partition, channel(), size);
    }
}
