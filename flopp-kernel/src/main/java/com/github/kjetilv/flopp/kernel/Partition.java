package com.github.kjetilv.flopp.kernel;

import static com.github.kjetilv.flopp.kernel.Partitioning.ALIGNMENT;

public record Partition(int partitionNo, int partitionCount, long offset, long count)
    implements Comparable<Partition> {

    public Partition(int partitionNo, int partitionCount, long offset, long count) {
        this.partitionNo = Non.negative(partitionNo, "partitionNo");
        this.partitionCount = Non.negativeOrZero(partitionCount, "partitionCount");
        this.offset = Non.negative(offset, "offset");
        this.count = Non.negative(count, "count");
        if (partitionNo >= partitionCount) {
            throw new IllegalStateException(
                STR."partitionNo >= partitionCount: \{partitionNo} >= \{partitionCount}"
            );
        }
    }

    public long length(Shape shape) {
        if (shape.limitsLineLength()) {
            return Math.min(
                shape.size() - offset,
                bufferedTo(shape.longestLine() + 1)
            );
        }
        throw new IllegalStateException(STR."Shape does not specify max line length: \{shape}");
    }

    @Override
    public int compareTo(Partition o) {
        return Integer.compare(partitionNo, o.partitionNo);
    }

    public Partition at(long offset, long count) {
        return new Partition(partitionNo, partitionCount, offset, count);
    }

    public boolean first() {
        return partitionNo == 0;
    }

    public boolean hasData() {
        return count > 0;
    }

    public boolean last() {
        return partitionNo == partitionCount - 1;
    }

    @Override
    public String toString() {
        String pos1 = first() ? "<" : "";
        String pos2 = last() ? ">" : "";
        String frac = first() || last() ? "" : STR."/\{partitionCount}";
        return STR."\{getClass().getSimpleName()}[\{pos1}\{partitionNo + 1}\{frac}\{pos2}@\{offset}+\{count}]";
    }

    public long bufferedTo(long size) {
        long simpleBuffer = count + size;
        if (simpleBuffer % ALIGNMENT == 0) {
            return simpleBuffer;
        }
        return (simpleBuffer / ALIGNMENT + 1) * ALIGNMENT;
    }
}
