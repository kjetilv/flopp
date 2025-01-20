package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Bits;
import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;

final class BitwiseCsvQuotedSplitter extends AbstractBitwiseCsvLineSplitter {

    private final Bits.Finder quoFinder;

    private int state;

    BitwiseCsvQuotedSplitter(Consumer<SeparatedLine> lines, Format.Csv.Quoted format) {
        super(lines, format);
        this.quoFinder = Bits.finder(format.quote(), format.fast());
    }

    @Override
    protected String substring() {
        return formatString() + " " + state;
    }

    @Override
    protected void inited() {
        state = STARTING_COLUMN;
    }

    protected void findSeps(long offset, long data, long endOffset) {
        int sep = nextSep(data);
        int quo = quoFinder.next(data);
        while (true) {
            int diff = sep - quo;
            if (diff == 0 || offset + sep > endOffset) {
                return;
            }
            if (diff < 0) {
                handleSep(offset + sep);
                sep = nextSep();
            } else {
                handleQuo();
                quo = quoFinder.next();
            }
        }
    }

    private void handleSep(long index) {
        switch (state) {
            case STARTING_COLUMN -> markSeparator(index);
            case QUOTING_QUOTE -> {
                markSeparator(index);
                state = STARTING_COLUMN;
            }
        }
    }

    private void handleQuo() {
        state = switch (state) {
            case STARTING_COLUMN, QUOTING_QUOTE -> QUOTING_COLUMN;
            case QUOTING_COLUMN -> QUOTING_QUOTE;
            default -> throw new IllegalStateException("Wrong state: " + state);
        };
    }

    private static final int STARTING_COLUMN = 1;

    private static final int QUOTING_QUOTE = 2;

    private static final int QUOTING_COLUMN = 4;
}
