package com.github.kjetilv.lopp;

import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

final class PartitionSpliterator extends Spliterators.AbstractSpliterator<NpLine> {

    private final Partition partition;

    private final ByteSource byteSource;

    private final Charset charset;

    private final SurroundConsumer<NpLine> surroundConsumer;

    private final int partitionNo;

    private final long partitionSize;

    private final int headerCount;

    private final int footerCount;

    private final boolean firstPartition;

    private final boolean lastPartition;

    private final byte[] scanBytes;

    private final long size;

    private boolean firstLineFound;

    private Slice currentSlice;

    private int traversed;

    private long traverseLimit;

    private long nextLineNo;

    private int shipped;

    private boolean trailing;

    private byte[] lineBytes;

    private int lineIndex;

    private int maxLength;

    PartitionSpliterator(
        ByteSource byteSource,
        Partition partition,
        Shape shape,
        int bufferSize
    ) {
        super(Long.MAX_VALUE, ORDERED | IMMUTABLE);
        Objects.requireNonNull(shape, "shape");
        this.size = shape.size();
        this.byteSource = Objects.requireNonNull(byteSource, "bytesProvider");
        this.partition = Objects.requireNonNull(partition, "partition");
        this.partitionSize = this.partition.count();
        this.firstPartition = this.partition.first();
        this.lastPartition = this.partition.last();
        this.partitionNo = partition.partitionNo();
        this.maxLength = longestLine(shape);
        this.traverseLimit = computeTraverseLimit();
        this.currentSlice = Slice.first(
            Non.negativeOrZero(bufferSize, "bufferSize"),
            traverseLimit
        );
        this.scanBytes = new byte[bufferSize];
        this.lineBytes = new byte[maxLength];
        this.charset = shape.charset();
        this.firstLineFound = partition.first();

        this.headerCount = shape.header();
        this.footerCount = shape.footer();
        this.surroundConsumer = SurroundConsumers.surround(
            firstPartition && headerCount > 0 ? headerCount : 0,
            lastPartition && footerCount > 0 ? footerCount : 0
        );
    }

    @Override
    public boolean tryAdvance(Consumer<? super NpLine> action) {
        try {
            int bytesToRead = byteSource.fill(scanBytes, currentSlice.offset(), currentSlice.length());
            int firstLineIndex = 0;
            if (!firstLineFound) {
                for (int i = 0; i < bytesToRead && !firstLineFound; i++) {
                    try {
                        byte b = scanBytes[i];
                        if (b == '\n') {
                            firstLineIndex = i + 1;
                            firstLineFound = true;
                        }
                    } finally {
                        traversed++;
                    }
                }
                if (!firstLineFound || firstLineIndex == bytesToRead) {
                    currentSlice = currentSlice.next();
                    return currentSlice.length() > 0;
                }
            }
            for (int i = firstLineIndex; i < bytesToRead; i++) {
                if (trailing && traversed - lineIndex > partitionSize) {
                    return done();
                }
                try {
                    byte b = scanBytes[i];
                    if (b == '\n') {
                        String line = new String(
                            lineBytes,
                            0,
                            lineIndex,
                            charset
                        );
                        NpLine npLine = npLine(line);
                        try {
                            ship(action, npLine);
                        } finally {
                            shipped++;
                            nextLineNo++;
                            lineIndex = 0;
                        }
                        if (trailing) {
                            return done();
                        }
                    } else {
                        try {
                            if (lineIndex == maxLength) {
                                growLineBuffer();
                            }
                            this.lineBytes[lineIndex] = b;
                        } finally {
                            lineIndex++;
                        }
                    }
                } finally {
                    traversed++;
                }
                if (!trailing && traversed > partitionSize) {
                    if (lastPartition) {
                        return done();
                    }
                    trailing = true;
                }
            }
            if (lastPartition && traversed == partitionSize) {
                return done();
            }
            currentSlice = currentSlice.next();
            if (currentSlice.length() == 0) {
                if (trailing) {
                    return false;
                }
                throw new IllegalStateException("Reading past allocated size");
            }
            return true;
        } catch (Exception e) {
            throw new IllegalStateException(this + ": Failed to advance in partition", e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + ": " + shipped + " lines]";
    }

    @SuppressWarnings("unchecked")
    private void ship(Consumer<? super NpLine> action, NpLine npLine) {
        surroundConsumer.accept((Consumer<NpLine>) action, npLine);
    }

    private NpLine npLine(String line) {
        return new NpLine(line, partitionNo, nextLineNo);
    }

    private boolean done() {
        if (firstPartition && shipped < headerCount) {
            throw new IllegalStateException(this + ": Partition is shorter than header");
        }
        if (lastPartition && shipped < footerCount) {
            throw new IllegalStateException(this + ": Partition is shorter than footer");
        }
        return false;
    }

    private void growLineBuffer() {
        this.maxLength *= 2;
        byte[] newCurrentLinebytes = new byte[this.maxLength];
        System.arraycopy(
            this.lineBytes,
            0,
            newCurrentLinebytes,
            0,
            this.lineIndex
        );
        this.traverseLimit = computeTraverseLimit();
        this.currentSlice = this.currentSlice.bump(traverseLimit);
        this.lineBytes = newCurrentLinebytes;
    }

    private long computeTraverseLimit() {
        return Math.min(
            partitionSize + (lastPartition ? 0 : this.maxLength),
            size - this.partition.offset()
        );
    }

    private static final int DEFAULT_LONGEST_LINE = 1024;

    private static int longestLine(Shape shape) {
        Shape.Stats stats = shape.stats();
        return stats != null && stats.longestLine() > 0 ? stats.longestLine() : DEFAULT_LONGEST_LINE;
    }
}
