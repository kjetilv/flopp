package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.ByteSource;
import com.github.kjetilv.flopp.kernel.ByteSources;
import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.Partition;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

public final class FileChannelByteSources extends AbstractFileChanneling implements ByteSources {

    private final long size;

    private final int padding;

    public FileChannelByteSources(Path path, long size) {
        this(path, size, Math.toIntExact(Math.max(10L, size / 10)));
    }

    public FileChannelByteSources(Path path, long size, int padding) {
        super(path, false);
        this.size = Non.negative(size, "size");
        this.padding = Non.negativeOrZero(padding, "padding");
    }

    @Override
    public ByteSource source(Partition partition) {
        return new FileChannelByteSource(partition, randomAccess(), size, padding);
    }
}
