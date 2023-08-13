package com.github.kjetilv.lopp;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class PartitionTransferer implements Consumer<FileChannel> {

    private final Path source;

    private final Partition partition;

    private final AtomicBoolean done = new AtomicBoolean();

    public PartitionTransferer(Path source, Partition partition) {
        this.source = Objects.requireNonNull(source, "source");
        this.partition = Objects.requireNonNull(partition, "partition");
    }

    @Override
    public void accept(FileChannel target) {
        if (done.compareAndSet(false, true)) {
            try (
                RandomAccessFile sourceFile = new RandomAccessFile(source.toFile(), "r");
                FileChannel source = sourceFile.getChannel()
            ) {
                long totalTransferred = 0L;
                do {
                    totalTransferred += target.transferFrom(
                        source,
                        partition.offset(),
                        partition.count()
                    );
                } while (totalTransferred < partition.count());
            } catch (Exception e) {
                throw new IllegalStateException("Failed to write " + source, e);
            }
        }
    }

    @Override public String toString() {
        return getClass().getSimpleName() + "[" + source + ", " + partition + "]";
    }
}
