package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.Objects;
import java.util.function.Consumer;

@SuppressWarnings("DuplicatedCode")
final class BitwiseSimpleCsvLineSplitter extends AbstractBitwiseCsvLineSplitter {

    BitwiseSimpleCsvLineSplitter(
        Consumer<SeparatedLine> lines,
        CsvFormat.Simple format,
        boolean immutable
    ) {
        super(lines, format, immutable);
    }

    @Override
    public void accept(LineSegment segment) {
        this.offset = this.currentStart = this.columnNo = 0;
        this.segment = Objects.requireNonNull(segment, "segment");
        this.startOffset = this.segment.startIndex();

        long length = this.segment.length();
        if (length < ALIGNMENT) {
            findSeps(this.segment.bytesAt(0, length), 0);
            addSep(length);
        } else {
            processHead();
            long longCount = this.segment.alignedCount();
            for (int i = 1; i < longCount; i++) {
                findSeps(this.segment.longNo(i), 0);
            }
            if (this.segment.isAlignedAtEnd()) {
                addSep(length);
            } else {
                findSeps(this.segment.tail(true), 0);
                addSep(length);
            }
        }
        emit();
    }

    private void processHead() {
        long headStart = this.segment.headStart();
        if (headStart == 0) {
            long headLong = this.segment.longNo(0);
            findSeps(headLong, 0);
        } else {
            offset = -headStart;
            long headLong = this.segment.head(headStart);
            findSeps(headLong, headStart);
        }
    }

    private void findSeps(long bytes, long shift) {
        int nextSep = sepFinder.next(bytes);
        while (true) {
            if (nextSep == ALIGNMENT) { // No match
                offset += ALIGNMENT;
                return;
            }
            handleSep(nextSep + shift);
            nextSep = sepFinder.next();
        }
    }

    private void handleSep(long index) {
        markColumn(index);
    }

    private void markColumn(long index) {
        long end = offset + index;
        addSep(end);
        currentStart = end + 1;
    }

    private void addSep(long end) {
        try {
            this.startPositions[columnNo] = startOffset + currentStart;
            this.endPositions[columnNo] = startOffset + end;
            this.columnNo++;
        } catch (Exception e) {
            throw new IllegalStateException(this + " could not set column #" + (columnNo + 1), e);
        }
    }
}
