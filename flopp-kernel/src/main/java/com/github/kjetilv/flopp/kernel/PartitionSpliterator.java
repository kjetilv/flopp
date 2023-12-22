package com.github.kjetilv.flopp.kernel;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

final class PartitionSpliterator extends Spliterators.AbstractSpliterator<NpLine> {

    /**
     * This is our partition. There are many like it, but this is ours
     */
    private final Partition partition;

    /**
     * The source of our bytes
     */
    private final ByteSource byteSource;

    /**
     * Charset for building line strings
     */
    private final Charset charset;

    /**
     * A convenient consumer for the lines, managing header/footer counts.
     */
    private final SurroundConsumer<NpLine> lineConsumer;

    private final int partitionNo;

    /**
     * The size in bytes of this partition
     */
    private final long partitionSize;

    /**
     * Number of header lines in the partitioned data
     */
    private final int headerCount;

    /**
     * Number of footer lines in the partitioned data
     */
    private final int footerCount;

    /**
     * Iff true, this is the first partition
     */
    private final boolean firstPartition;

    /**
     * Iff true, this is the last partition
     */
    private final boolean lastPartition;

    /**
     * Buffer holding the current working set of bytes
     */
    private final byte[] byteBuffer;

    /**
     * Total size of all the partitioned data.
     */
    private final long size;

    /**
     * Flag indicating that we've found the first line in the partition
     */
    private boolean firstLineFound;

    /**
     * The current slice being processed.
     */
    private Slice currentSlice;

    /**
     * The number of bytes we've processed so far
     */
    private int traversed;

    /**
     * The maximum number of bytes we should expect to ever have to read, given the current {@link #maxLineLength}
     */
    private long traverseLimit;

    /**
     * Line number of next line
     */
    private long nextLineNo = 1;

    /**
     * Number of lines shipped
     */
    private int shipped;

    /**
     * Iff true, we're currently past our allocated byte range. The current line is our last
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
        Objects.requireNonNull(shape, "shape");
        this.size = shape.size();
        this.byteSource = Objects.requireNonNull(byteSource, "bytesProvider");
        this.partition = Objects.requireNonNull(partition, "partition");
        this.partitionSize = this.partition.count();
        this.firstPartition = this.partition.first();
        this.lastPartition = this.partition.last();
        this.partitionNo = partition.partitionNo();
        this.maxLineLength = longestLine(shape); // If the shape indicates a longest line, make note of it
        this.traverseLimit = computeTraverseLimit();
        this.currentSlice = Slice.first(
            Non.negativeOrZero(bufferSize, "bufferSize"),
            traverseLimit
        );
        this.byteBuffer = new byte[bufferSize];
        this.lineBytes = new byte[maxLineLength];
        this.charset = shape.charset();
        this.firstLineFound = partition.first();

        this.headerCount = shape.header();
        this.footerCount = shape.footer();
        this.lineConsumer = SurroundConsumers.surround(
            firstPartition && headerCount > 0 ? headerCount : 0,
            lastPartition && footerCount > 0 ? footerCount : 0
        );
    }

    @Override
    public boolean tryAdvance(Consumer<? super NpLine> action) {
        try {
            int bytesToRead = byteSource.fill(byteBuffer, currentSlice.offset(), currentSlice.length());
            int firstLineIndex = 0;
            if (!firstLineFound) { // Still haven't found first line, still on the previous partition's trail
                for (int i = 0; i < bytesToRead && !firstLineFound; i++) { // Fast forward ...
                    try {
                        byte byyte = byteBuffer[i]; // Ok, so next byte is ...
                        if (byyte == '\n') { // Found it!
                            firstLineIndex = i + 1;
                            firstLineFound = true;
                        }
                    } finally {
                        traversed++; // Count up number of bytes processed
                    }
                }
                if (!firstLineFound || firstLineIndex == bytesToRead) { // Still no line!
                    currentSlice = currentSlice.next(); // Move to the next slice then
                    return currentSlice.length() > 0; // If that slice is empty – guess we're done then
                }
            }
            for (int i = firstLineIndex; i < bytesToRead; i++) { // Found first line, now onwards!
                if (trailing && traversed > partitionSize + lineIndex) { // What does this mean?
                    return done(); // Looks like we are done, actually - but why!
                }
                try {
                    byte byyte = byteBuffer[i]; // So what's the next byyte then?
                    if (byyte == '\n') { // We've got a line!
                        try {
                            NpLine npLine = npLine(extract());
                            ship(action, npLine); // Here it is!
                        } finally {
                            shipped++; // Count up lines shipped
                            nextLineNo++; // Note the next line number
                            lineIndex = 0; // Note that we're beginning a new line
                        }
                        if (trailing) { // This was the line on the trailing end of our partition, so we are done
                            return done();
                        }
                    } else { // No line yet
                        try {
                            if (lineIndex == maxLineLength) { // This is a big line, we need more space to hold it
                                this.maxLineLength *= 2; // Let's double it
                                byte[] newCurrentLinebytes = copyToNewBuffer( // Make a new buffer, hold
                                    this.lineBytes,
                                    this.lineIndex,
                                    this.maxLineLength
                                );
                                this.lineBytes = newCurrentLinebytes; // This new buffer should do
                                this.traverseLimit = computeTraverseLimit(); // Re-compute limit
                                this.currentSlice = this.currentSlice.newTotal(traverseLimit); // Adjust slice
                            }
                            this.lineBytes[lineIndex] = byyte; // Remember the byte for the upcoming line
                        } finally {
                            lineIndex++; // Count up our position on the current line
                        }
                    }
                } finally {
                    traversed++; // Whatever we did, count up number of bytes processed
                }
                if (traversed > partitionSize && !trailing) { // We are past our byte mark!
                    trailing = true; // Make a note that we are now in the trailing part of the partition
                    if (lastPartition) { // This is the last partition
                        return done(); // So it goes
                    }
                }
            }
            if (lastPartition && traversed == partitionSize) { // We've exhausted the last partition
                return done(); // So that's it
            }
            Slice nextSlice = currentSlice.next();
            if (nextSlice.done()) { // Oops, it's empty
                if (trailing) { // That's ok, we are in the trailing end
                    return done(); // So we are done
                }
                throw new IllegalStateException(
                    "Reading past allocated size of " + partition + ": " + partitionSize); // We're not? Something's up!
            }
            currentSlice = nextSlice; // We have another slice coming up.
            return true; // Keep going!
        } catch (Exception e) {
            throw new IllegalStateException(this + ": Failed to advance in partition", e); // SOMETHING's up.
        }
    }

    private String extract() {
        return new String(lineBytes, 0, lineIndex, charset);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + partition + ": " + shipped + " lines]";
    }

    private long computeTraverseLimit() {
        return lastPartition
            ? size - this.partition.offset()
            : partitionSize + this.maxLineLength;
    }

    @SuppressWarnings("unchecked")
    private void ship(Consumer<? super NpLine> action, NpLine npLine) {
        lineConsumer.accept((Consumer<NpLine>) action, npLine);
    }

    private NpLine npLine(String line) {
        return new NpLine(line, partitionNo, nextLineNo);
    }

    private boolean done() {
        if (firstPartition && shipped < headerCount) { // Check if we've got a bad partition
            throw new IllegalStateException(this + ": Partition is shorter than header");
        }
        if (lastPartition && shipped < footerCount) { // Or a really bad one
            throw new IllegalStateException(this + ": Partition is shorter than footer");
        }
        return false;
    }

    private static final int DEFAULT_LONGEST_LINE = 1024;

    private static byte[] copyToNewBuffer(byte[] src, int index, int length) {
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
