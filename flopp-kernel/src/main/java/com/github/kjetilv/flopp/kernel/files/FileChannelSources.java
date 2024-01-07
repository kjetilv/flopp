package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.ByteSource;
import com.github.kjetilv.flopp.kernel.ByteSources;
import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.Partition;

import java.nio.file.Path;

public final class FileChannelSources extends AbstractFileChanneling implements ByteSources {

    private final long size;

    private final int padding;

    public FileChannelSources(Path path, long size) {
        this(path, size, Math.toIntExact(Math.max(10L, size / 10)));
    }

    public FileChannelSources(Path path, long size, int padding) {
        super(path, false);
        this.size = Non.negative(size, "size");
        this.padding = Non.negativeOrZero(padding, "padding");
    }

    @Override
    public ByteSource source(Partition partition) {
        return new FileChannelSource(partition, channel(), size, padding);
    }
}
