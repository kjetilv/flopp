package com.github.kjetilv.lopp.files;

import com.github.kjetilv.lopp.ByteSource;
import com.github.kjetilv.lopp.ByteSources;
import com.github.kjetilv.lopp.Non;
import com.github.kjetilv.lopp.Partition;

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
