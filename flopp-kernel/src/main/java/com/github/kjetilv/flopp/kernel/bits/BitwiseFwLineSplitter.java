package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.FwFormat;
import com.github.kjetilv.flopp.kernel.Range;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.function.Consumer;

final class BitwiseFwLineSplitter extends AbstractBitwiseLineSplitter {

    private final long[] start;

    private final long[] end;

    private final int length;

    BitwiseFwLineSplitter(FwFormat format, Consumer<SeparatedLine> lines) {
        super(lines);
        this.length = format.ranges().length;
        this.start = Arrays.stream(format.ranges()).mapToLong(Range::startIndex).toArray();
        this.end = Arrays.stream(format.ranges()).mapToLong(Range::endIndex).toArray();
    }

    @Override
    public int columnCount() {
        return length;
    }

    @Override
    public String column(int column, Charset charset) {
        return "";
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
    public long start(int column) {
        return start[column];
    }

    @Override
    public long end(int column) {
        return end[column];
    }
}
