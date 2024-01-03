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
     * The current slice being processed.
     */
    protected Slice slice;

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
    protected int pointer;

    /**
     * Line number of next line
     */
    protected long nextLineNo = 1;

    /**
     * Number of lines shipped
     */
    private int shipped;

    /**
     * Iff true, we're currently past our allocated byte range, and the current line is our last
     */
    private boolean trailing;

    public AbstractPartitionSpliterator(
        int bufferSize,
        ByteSource byteSource,
        Partition partition,
        Shape shape
    ) {
        super(Long.MAX_VALUE, ORDERED | IMMUTABLE);
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
        this.limit = computeSliceLength();
        this.slice = Slice.first(
            Non.negativeOrZero(bufferSize, "bufferSize"),
            this.limit
        );

        this.maxLineLength = longestLine(this.shape); // If the shape indicates a longest line, make note of it
        this.lineBytes = new byte[maxLineLength];
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        try {
            long bytesToRead = byteSource.fill(byteBuffer, slice.offset(), slice.length());
            int firstLineIndex = 0;
            if (!firstLineFound) { // Still haven't found first line, still on the previous partition's trail
                for (int i = 0; i < bytesToRead && !firstLineFound; i++) { // Fast forward ...
                    byte c = byteBuffer[i]; // Ok, so next byte is ...
                    if (c == '\n') { // Found it!
                        firstLineIndex = i + 1;
                        firstLineFound = true;
                    }
                    pointer++; // Count up number of bytes processed
                }
                if (!firstLineFound || firstLineIndex == bytesToRead) { // Still no line!
                    if (slice.last(limit)) {
                        return false;
                    }
                    slice = slice.next(limit);
                    return true;
                }
            }
            for (int i = firstLineIndex; i < bytesToRead; i++) { // Found first line, now onwards!
                byte c = byteBuffer[i]; // So what's the next byyte then?
                if (c == '\n') { // We've got a line!
                    ship(action, item()); // Here it is!
                    shipped++; // Count up lines shipped
                    nextLineNo++; // Note the next line number
                    lineIndex = 0; // Note that we're beginning a new line
                    if (trailing) { // This was the line on the trailing end of our partition, so we are done
                        return done();
                    }
                } else { // No line yet
                    if (lineIndex == maxLineLength) { // This is a big line, we need more space to hold it
                        growBuffer();
                    }
                    lineBytes[lineIndex] = c; // Remember the byte for the upcoming line
                    lineIndex++; // Count up our position on the current line
                }
                pointer++; // Whatever we did, count up number of bytes processed
                if (trailing) {
                    if (pointer > partition.count() + lineIndex) { // What does this mean?
                        return done(); // Looks like we are done, actually - but why!
                    }
                } else {
                    if (pointer > partition.count()) { // We are past our byte mark!
                        if (partition.last()) { // This is the last partition
                            return done(); // So it goes
                        }
                        trailing = true; // Make a note that we are now in the trailing part of the partition
                    }
                }
            }
            if (partition.last() && pointer == partition.count()) { // We've exhausted the last partition
                return done(); // So that's it
            }
            if (slice.last(limit)) { // Oops, it's empty
                growBuffer();
                slice = slice.next(computeSliceLength()); // Adjust slice
            } else {
                slice = slice.next(limit);
            }
            return true; // Keep going!
        } catch (Exception e) {
            throw new IllegalStateException(this + ": Failed to advance in partition", e); // SOMETHING's up.
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + "]";
    }

    protected abstract T item();

    private long computeSliceLength() {
        return partition.last()
            ? shape.size() - partition.offset()
            : partition.count() + maxLineLength;
    }

    private void growBuffer() {
        maxLineLength *= 2; // Let's double it
        byte[] newCurrentLinebytes = new byte[maxLineLength];
        System.arraycopy(lineBytes, 0, newCurrentLinebytes, 0, lineIndex);
        lineBytes = newCurrentLinebytes; // This new buffer should do
        limit = computeSliceLength();
    }

    @SuppressWarnings("unchecked")
    private void ship(Consumer<? super T> action, T t) {
        lineConsumer.accept((Consumer<T>) action, t);
    }

    private boolean done() {
        if (partition.first() && shipped < shape.header()) { // Check if we've got a bad partition
            throw new IllegalStateException(this + ": Partition is shorter than header");
        }
        if (partition.last() && shipped < shape.footer()) { // Or a really bad one
            throw new IllegalStateException(this + ": Partition is shorter than footer");
        }
        return false;
    }

    private static final int DEFAULT_LONGEST_LINE = 1024;

    protected static int longestLine(Shape shape) {
        Shape.Stats stats = shape.stats();
        return stats != null && stats.longestLine() > 0
            ? stats.longestLine()
            : DEFAULT_LONGEST_LINE;
    }
}
