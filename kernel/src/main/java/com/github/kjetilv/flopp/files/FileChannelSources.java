package com.github.kjetilv.flopp.files;

import com.github.kjetilv.flopp.ByteSource;
import com.github.kjetilv.flopp.ByteSources;
import com.github.kjetilv.flopp.Non;
import com.github.kjetilv.flopp.Partition;

import java.nio.file.Path;

public final class FileChannelSources extends FileChannelBase implements ByteSources {

    private final long size;

    public FileChannelSources(Path path, long size) {
        super(path, false);
        this.size = Non.negative(size, "size");
    }

    @Override
    public ByteSource source(Partition partition) {
        return new FileChannelSource(partition, channel(), size);
    }
}
