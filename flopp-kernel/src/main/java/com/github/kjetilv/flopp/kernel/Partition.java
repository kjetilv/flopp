package com.github.kjetilv.flopp.kernel;

import java.util.List;

import static com.github.kjetilv.flopp.kernel.Partitioning.ALIGNMENT;

public record Partition(int partitionNo, int partitionCount, long offset, long length)
    implements Comparable<Partition>, Range {

    public Partition(int partitionNo, int partitionCount, long offset, long length) {
        this.partitionNo = Non.negative(partitionNo, "partitionNo");
        this.partitionCount = Non.negativeOrZero(partitionCount, "partitionCount");
        this.offset = Non.negative(offset, "offset");
        this.length = Non.negative(length, "count");
        if (partitionNo >= partitionCount) {
            throw new IllegalStateException("partitionNo >= partitionCount: " + partitionNo + " >= " + partitionCount);
        }
    }

    public long length(Shape shape) {
        return Math.min(
            shape.size() - offset,
            shape.limitsLineLength()
                ? bufferedTo(shape.longestLine() + 1)
                : length
        );
    }

    @Override
    public int compareTo(Partition o) {
        return Integer.compare(partitionNo, o.partitionNo);
    }

    public Partition at(long offset, long count) {
        return new Partition(partitionNo, partitionCount, offset, count);
    }

    public List<Partition> smash(int fragments) {
        if (last()) {
            return List.of(this);
        }
        return Partitioning.create(fragments).of(length);
    }

    public boolean first() {
        return partitionNo == 0;
    }

    public boolean last() {
        return partitionNo == partitionCount - 1;
    }

    @Override
    public long startIndex() {
        return offset();
    }

    @Override
    public long endIndex() {
        return offset() + length();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" +
               str() + " " + offset + "+" + length +
               "]";
    }

    public long bufferedTo(long size) {
        long simpleBuffer = length + size;
        if (simpleBuffer % ALIGNMENT == 0) {
            return simpleBuffer;
        }
        return (simpleBuffer / ALIGNMENT + 1) * ALIGNMENT;
    }

    public boolean single() {
        return partitionCount == 1;
    }

    private String str() {
        boolean f = first();
        boolean l = last();
        if (f && l) {
            return "<>";
        }
        String pos = f ? "<" : l ? ">" : "";
        Object no = f || l ? "" : partitionNo;
        String s = pos + no + "/" + (partitionCount - 1);
        return s;
    }
}
