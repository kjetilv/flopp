package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

final class PartitionSpliterator extends Spliterators.AbstractSpliterator<NpLine> {

    /**
     * The source of our bytes
     */
    private final ByteSource byteSource;

    /**
     * This is our partition. There are many like it, but this is ours
     */
    private final Partition partition;

    /**
     * Shape of file
     */
    private final Shape shape;

    /**
     * Charset for building line strings
     */
    private final Charset charset;

    /**
     * A convenient consumer for the lines, managing header/footer counts.
     */
    private final SurroundConsumer<NpLine> lineConsumer;

    /**
     * Buffer holding the current working set of bytes
     */
    private final byte[] byteBuffer;

    /**
     * Flag indicating that we've found the first line in the partition
     */
    private boolean firstLineFound;

    /**
     * The current slice being processed.
     */
    private Slice slice;

    /**
     * The number of bytes we've processed so far
     */
    private int traversed;

    /**
     * Line number of next line
     */
    private long nextLineNo = 1;

    /**
     * Number of lines shipped
     */
    private int shipped;

    /**
     * Iff true, we're currently past our allocated byte range, and the current line is our last
     */
    private boolean trailing;

    /**
     * Byte array holding the current line being read
     */
    private byte[] lineBytes;

    /**
     * Which index we're currently at in the {@link #lineBytes}
     */
    private int lineIndex;

    /**
     * The maximum length of a line (yet).
     */
    private int maxLineLength;

    PartitionSpliterator(
        ByteSource byteSource,
        Partition partition,
        Shape shape,
        int bufferSize
    ) {
        super(Long.MAX_VALUE, ORDERED | IMMUTABLE);
        this.byteSource = Objects.requireNonNull(byteSource, "bytesProvider");
        this.partition = Objects.requireNonNull(partition, "partition");
        this.shape = Objects.requireNonNull(shape, "shape");

        this.maxLineLength = longestLine(this.shape); // If the shape indicates a longest line, make note of it
        this.slice = Slice.first(
            Non.negativeOrZero(bufferSize, "bufferSize"),
            computeSliceLength()
        );
        this.byteBuffer = new byte[bufferSize];
        this.lineBytes = new byte[maxLineLength];
        this.charset = this.shape.charset();
        this.firstLineFound = this.partition.first();

        this.lineConsumer = SurroundConsumers.surround(
            this.partition.first() && this.shape.header() > 0 ? this.shape.header() : 0,
            this.partition.last() && this.shape.footer() > 0 ? this.shape.footer() : 0
        );
    }

    @Override
    public boolean tryAdvance(Consumer<? super NpLine> action) {
        try {
            long bytesToRead = byteSource.fill(byteBuffer, slice.offset(), slice.length());
            int firstLineIndex = 0;
            if (!firstLineFound) { // Still haven't found first line, still on the previous partition's trail
                for (int i = 0; i < bytesToRead && !firstLineFound; i++) { // Fast forward ...
                    byte byyte = byteBuffer[i]; // Ok, so next byte is ...
                    if (byyte == '\n') { // Found it!
                        firstLineIndex = i + 1;
                        firstLineFound = true;
                    }
                    traversed++; // Count up number of bytes processed
                }
                if (!firstLineFound || firstLineIndex == bytesToRead) { // Still no line!
                    Slice next = slice.next();
                    if (next.done()) {
                        return false;
                    }
                    slice = next; // Move to the next slice then
                    return true; // If that slice is empty – guess we're done then
                }
            }
            for (int i = firstLineIndex; i < bytesToRead; i++) { // Found first line, now onwards!
                if (trailing && traversed > partition.count() + lineIndex) { // What does this mean?
                    return done(); // Looks like we are done, actually - but why!
                }
                byte byyte = byteBuffer[i]; // So what's the next byyte then?
                if (byyte == '\n') { // We've got a line!
                    String line = new String(lineBytes, 0, lineIndex, charset);
                    ship(action, new NpLine(line, partition.partitionNo(), nextLineNo)); // Here it is!
                    shipped++; // Count up lines shipped
                    nextLineNo++; // Note the next line number
                    lineIndex = 0; // Note that we're beginning a new line
                    if (trailing) { // This was the line on the trailing end of our partition, so we are done
                        return done();
                    }
                } else { // No line yet
                    if (lineIndex == maxLineLength) { // This is a big line, we need more space to hold it
                        growBuffer();
                        slice = slice.withTotal(computeSliceLength()); // Adjust slice
                    }
                    lineBytes[lineIndex] = byyte; // Remember the byte for the upcoming line
                    lineIndex++; // Count up our position on the current line
                }
                traversed++; // Whatever we did, count up number of bytes processed
                if (traversed > partition.count() && !trailing) { // We are past our byte mark!
                    trailing = true; // Make a note that we are now in the trailing part of the partition
                    if (partition.last()) { // This is the last partition
                        return done(); // So it goes
                    }
                }
            }
            if (partition.last() && traversed == partition.count()) { // We've exhausted the last partition
                return done(); // So that's it
            }
            if (slice.last()) { // Oops, it's empty
                growBuffer();
            }
            slice = slice.next(computeSliceLength()); // Adjust slice
            return true; // Keep going!
        } catch (Exception e) {
            throw new IllegalStateException(this + ": Failed to advance in partition", e); // SOMETHING's up.
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + ": " + shipped + " lines]";
    }

    private void growBuffer() {
        maxLineLength *= 2; // Let's double it
        lineBytes = expandBuffer(lineBytes, lineIndex, maxLineLength); // This new buffer should do
    }

    private long computeSliceLength() {
        return partition.last()
            ? shape.size() - partition.offset()
            : partition.count() + maxLineLength;
    }

    @SuppressWarnings("unchecked")
    private void ship(Consumer<? super NpLine> action, NpLine npLine) {
        lineConsumer.accept((Consumer<NpLine>) action, npLine);
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

    private static byte[] expandBuffer(byte[] src, int index, int length) {
        byte[] newCurrentLinebytes = new byte[length];
        System.arraycopy(
            src,
            0,
            newCurrentLinebytes,
            0,
            index
        );
        return newCurrentLinebytes;
    }

    private static int longestLine(Shape shape) {
        Shape.Stats stats = shape.stats();
        return stats != null && stats.longestLine() > 0
            ? stats.longestLine()
            : DEFAULT_LONGEST_LINE;
    }
}
