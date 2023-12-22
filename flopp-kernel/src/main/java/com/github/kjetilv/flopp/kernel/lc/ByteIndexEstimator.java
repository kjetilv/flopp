package com.github.kjetilv.flopp.kernel.lc;

import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.lc.LineCounter.LineOffset;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

final class ByteIndexEstimator implements LineCounter.Lines {

    private final long[] bytePositions;

    private final long byteSize;

    private final Shape shape;

    private final Partitioning partitioning;

    private int index;

    private int headersLeft;

    private long linesCount;

    private int longestLine;

    private long lastPosition;

    private int factor;

    private int factorCountdown;

    ByteIndexEstimator(Shape shape, Partitioning partitioning, int scanResolution) {
        this.shape = Objects.requireNonNull(shape, "shape");
        this.byteSize = Non.negativeOrZero(shape.size(), "byteSize");
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.bytePositions = new long[estimationSlots(this.byteSize, scanResolution)];
        this.factor = 1;
        this.factorCountdown = 1;
        this.headersLeft = shape.header();
        if (shape.header() == 0) {
            lineStart(0);
        }
    }

    @Override
    public long linesCount() {
        return linesCount;
    }

    @Override
    public int longestLine() {
        return longestLine;
    }

    @Override
    public List<PartitionBytes> bytesPartitions() {
        LineOffset[] offsets = offsets(partitioning);
        int offsetsCount = offsets.length;
        int partitionCount = offsetsCount - 1;
        return IntStream.range(0, partitionCount).mapToObj(partitionNo -> {
                LineOffset lineOffset = offsets[partitionNo];
                long offset = lineOffset.lineNo();
                long byteOffset = lineOffset.bytePosition();

                LineOffset nextOffset = offsets[partitionNo + 1];
                Partition partition =
                    new Partition(partitionNo, partitionCount, offset, nextOffset.lineNo() - offset);
                long nextBytePosition = partition.last() ? byteSize : nextOffset.bytePosition();

                return new PartitionBytes(byteOffset, nextBytePosition - byteOffset, partition);
            })
            .toList();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() +
               "[@" + index + ":" +
               " factor=" + factor +
               " linesCount=" + linesCount +
               " longestLine=" + longestLine +
               "]";
    }

    void lineAt(long bytePosition) {
        try {
            if (headersLeft > 0) {
                headersLeft--;
            }
            if (headersLeft == 0) {
                factorCountdown--;
                if (factorCountdown == 0) {
                    if (index == bytePositions.length) {
                        compact();
                    }
                    lineStart(bytePosition);
                }
                trackLineLength(bytePosition);
            }
        } finally {
            linesCount++;
        }
    }

    private void lineStart(long position) {
        this.bytePositions[index] = position;
        factorCountdown = factor;
        index++;
    }

    private LineOffset[] offsets(Partitioning partitioning) {
        long[] idealPositions = idealPositions(partitioning.partitionCount());
        long[] approximateIndices = findApproximate(idealPositions);
        int header = shape.header();
        LineOffset[] offsets = new LineOffset[approximateIndices.length];
        for (int j = 0; j < approximateIndices.length; j++) {
            int approximateIndex = Math.toIntExact(approximateIndices[j]);
            long bytePosition = bytePositions[approximateIndex];
            boolean lastIndex = approximateIndices.length - j == 1;
            long lineNo = lastIndex
                ? linesCount - shape.footer()
                : header + (long) approximateIndex * factor;
            offsets[j] = new LineOffset(bytePosition, lineNo);
        }
        return offsets;
    }

    private long[] idealPositions(int partitionCount) {
        return LongStream.range(0, partitionCount + 1)
            .map(partitionNo -> partitionNo * byteSize / partitionCount)
            .toArray();
    }

    private long[] findApproximate(long[] idealPositions) {
        long[] approximations = new long[idealPositions.length];
        int currentApproximation = 0;
        long lastDistance = 0;
        for (int i = 0; i < this.index; i++) {
            long distance = idealPositions[currentApproximation] - bytePositions[i];
            if (distance == 0 || i == 0) {
                approximations[currentApproximation] = i;
                currentApproximation++;
                if (currentApproximation == approximations.length) {
                    return approximations;
                }
            } else if (distance > 0) {
                lastDistance = distance;
            } else { // Passed it!
                approximations[currentApproximation] = -distance > lastDistance ? i - 1 : i;
                currentApproximation++;
                if (currentApproximation == approximations.length) {
                    return approximations;
                }
            }
        }
        approximations[currentApproximation] = index - 1;
        return approximations;
    }

    private void compact() {
        try {
            for (int i = 0; i < bytePositions.length / 2; i++) {
                bytePositions[i] = bytePositions[i * 2];
            }
        } finally {
            index /= 2;
            factor *= 2;
        }
    }

    private void trackLineLength(long bytePosition) {
        long length = bytePosition - lastPosition;
        longestLine = Math.toIntExact(Math.max(longestLine, length));
        lastPosition = bytePosition;
    }

    private static int estimationSlots(long size, int resolution) {
        int desiredSlots = 2 * Math.toIntExact(size / resolution);
        if (size < Integer.MAX_VALUE) {
            return Math.min(Math.toIntExact(size), desiredSlots);
        }
        return desiredSlots;
    }
}
