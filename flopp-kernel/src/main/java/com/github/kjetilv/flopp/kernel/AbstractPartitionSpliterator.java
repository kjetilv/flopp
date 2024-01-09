package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("ProtectedField")
public abstract class AbstractPartitionSpliterator<T> extends Spliterators.AbstractSpliterator<T> {
    /**
     * The source of our bytes
     */
    protected final ByteSource byteSource;

    /**
     * This is our partition. There are many like it, but this is ours
     */
    protected final Partition partition;

    /**
     * Shape of file
     */
    protected final Shape shape;

    /**
     * Charset for building line strings
     */
    protected final Charset charset;

    /**
     * A convenient consumer for the lines, managing header/footer counts.
     */
    protected final BiConsumer<Consumer<T>, T> lineConsumer;

    /**
     * Buffer holding the current working set of bytes
     */
    protected final byte[] byteBuffer;

    /**
     * Flag indicating that we've found the first line in the partition
     */
    protected boolean firstLineFound;

    /**
     * Offset of the chunk being processed
     */
    protected long offset;

    /**
     * Length of the chunk being processed
     */
    protected int length;

    /**
     * The current limit for this partition
     */
    protected long limit;

    /**
     * Byte array holding the current line being read
     */
    protected byte[] lineBytes;

    /**
     * Which index we're currently at in the {@link #lineBytes}
     */
    protected int lineIndex;

    /**
     * The maximum length of a line (yet).
     */
    protected int maxLineLength;

    /**
     * The number of bytes we've processed so far
     */
    protected int partitionIndex;

    /**
     * Line number of next line
     */
    protected long nextLineNo = 1;

    protected final long partitionCount;

    protected final boolean lastPartition;

    /**
     * Iff true, we're currently past our allocated byte range, and the current line is our last
     */
    protected boolean trailing;

    /**
     * Number of lines shipped
     */
    private int shipped;

    public AbstractPartitionSpliterator(
        int bufferSize,
        ByteSource byteSource,
        Partition partition,
        Shape shape
    ) {
        super(
            Long.MAX_VALUE,
            ORDERED | IMMUTABLE
        );

        this.byteSource = requireNonNull(byteSource, "bytesProvider");
        this.partition = requireNonNull(partition, "partition");
        this.shape = requireNonNull(shape, "shape");

        this.charset = this.shape.charset();
        this.lineConsumer = SurroundConsumers.surround(
            this.partition.first() && this.shape.header() > 0 ? this.shape.header() : 0,
            this.partition.last() && this.shape.footer() > 0 ? this.shape.footer() : 0
        );
        this.byteBuffer = new byte[bufferSize];
        this.firstLineFound = this.partition.first();
        this.offset = 0;
        this.limit = computeLength();
        this.partitionCount = this.partition.count();
        this.lastPartition = this.partition.last();
        this.length = Math.toIntExact(Math.min(
            Non.negativeOrZero(bufferSize, "bufferSize"),
            this.limit
        ));

        this.maxLineLength = longestLine(this.shape); // If the shape indicates a longest line, make note of it
        this.lineBytes = new byte[maxLineLength];
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        try {
            return process(action);
        } catch (Exception e) {
            throw new IllegalStateException(this + ": Failed to advance in partition", e); // SOMETHING's up.
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + "]";
    }

    protected abstract T item();

    protected abstract boolean process(Consumer<? super T> action);

    protected final long computeLength() {
        return partition.last()
            ? shape.size() - partition.offset()
            : partition.count() + maxLineLength;
    }

    protected final void shipAndReset(Consumer<? super T> action) {
        ship(action, item()); // Here it is!
        shipped++; // Count up lines shipped
        nextLineNo++; // Note the next line number
        lineIndex = 0; // Note that we're beginning a new line
    }

    protected final boolean done() {
        if (partition.first() && shipped < shape.header()) { // Check if we've got a bad partition
            throw new IllegalStateException(this + ": Partition is shorter than header");
        }
        if (partition.last() && shipped < shape.footer()) { // Or a really bad one
            throw new IllegalStateException(this + ": Partition is shorter than footer");
        }
        return false;
    }

    protected boolean sliceDone() {
        return limit <= offset + length;
    }

    protected void nextSlice() {
        long nextOffset = offset + length;
        int nextLength = Math.toIntExact(Math.min(limit - nextOffset, length));
        offset = nextOffset;
        length = nextLength;
    }

    @SuppressWarnings("unchecked")
    private void ship(Consumer<? super T> action, T t) {
        lineConsumer.accept((Consumer<T>) action, t);
    }

    private static final int DEFAULT_LONGEST_LINE = 1024;

    protected static int longestLine(Shape shape) {
        Shape.Stats stats = shape.stats();
        return stats != null && stats.longestLine() > 0
            ? stats.longestLine()
            : DEFAULT_LONGEST_LINE;
    }
}
