package com.github.kjetilv.flopp.kernel;

public record Partition(int partitionNo, int partitionCount, long offset, long count, int alignment)
    implements Comparable<Partition> {

    public Partition(int partitionNo, int partitionCount, long offset, long count) {
        this(partitionNo, partitionCount, offset, count, 1);
    }

    public Partition(int partitionNo, int partitionCount, long offset, long count, int alignment) {
        this.partitionNo = Non.negative(partitionNo, "partitionNo");
        this.partitionCount = Non.negativeOrZero(partitionCount, "partitionCount");
        this.offset = Non.negative(offset, "offset");
        this.count = Non.negative(count, "count");
        if (partitionNo >= partitionCount) {
            throw new IllegalStateException(
                STR."partitionNo >= partitionCount: \{partitionNo} >= \{partitionCount}"
            );
        }
        this.alignment = Non.negativeOrZero(alignment, "alignment");
    }

    @Override
    public int compareTo(Partition o) {
        return Integer.compare(partitionNo, o.partitionNo);
    }

    public Partition at(long offset, long count) {
        return new Partition(partitionNo, partitionCount, offset, count, 1);
    }

    public boolean first() {
        return partitionNo == 0;
    }

    public boolean hasData() {
        return count > 0;
    }

    public boolean isLongAligned() {
        return alignment == 8 && isAligned();
    }

    public boolean isAligned() {
        return !last() && count % alignment == 0;
    }

    public boolean last() {
        return partitionNo == partitionCount - 1;
    }

    @Override
    public String toString() {
        String pos = first() ? "<"
            : last() ? ">"
                : "";
        String frac = first() || last() ? "" : STR."/\{partitionCount}";
        return STR."\{getClass().getSimpleName()}[\{pos}\{partitionNo + 1}\{frac}@\{offset}+\{count}]";
    }

    public long bufferedTo(int size) {
        long simpleBuffer = count + size;
        if (alignment == 1 || simpleBuffer % alignment == 0) {
            return simpleBuffer;
        }
        return (simpleBuffer / alignment + 1) * alignment;
    }
}
