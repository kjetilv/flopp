package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.Range;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.nio.charset.Charset;
import java.util.function.Consumer;

final class BitwiseFwLineSplitter extends AbstractBitwiseLineSplitter {

    private final long[] start;

    private final long[] end;

    private final int length;

    private int column;

    BitwiseFwLineSplitter(Format.FwFormat format, Consumer<SeparatedLine> lines) {
        super(lines);
        Range[] ranges = format.ranges();
        this.length = ranges.length;
        this.start = new long[this.length];
        this.end = new long[this.length];
        for (int i = 0; i < this.length; i++) {
            Range range = ranges[i];
            this.start[i] = range.startIndex();
            this.end[i] = range.endIndex();
        }
    }

    @Override
    public int columnCount() {
        return length;
    }

    @Override
    public long[] start() {
        return start;
    }

    @Override
    public long[] end() {
        return end;
    }

    @Override
    public String column(int column, Charset charset) {
        return "";
    }

    @Override
    public long start(int column) {
        return start[column];
    }

    @Override
    public long end(int column) {
        return end[column];
    }

    @Override
    public LineSegment segment(int column) {
        this.column = column;
        return this;
    }

    @Override
    public long startIndex() {
        return start[column];
    }

    @Override
    public long endIndex() {
        return end[column];
    }

    @Override
    protected String substring() {
        return null;
    }

    @Override
    protected void process(LineSegment segment) {
    }
}
