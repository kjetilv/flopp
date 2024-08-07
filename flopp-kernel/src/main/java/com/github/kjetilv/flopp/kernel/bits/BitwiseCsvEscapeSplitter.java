package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;

@SuppressWarnings("DuplicatedCode")
final class BitwiseCsvEscapeSplitter extends AbstractBitwiseCsvLineSplitter {

    private final Bits.Finder escFinder;

    private boolean escaping;

    private long nextEscape;

    BitwiseCsvEscapeSplitter(Consumer<SeparatedLine> lines, CsvFormat.Escape format) {
        super(lines, format);
        this.escFinder = Bits.finder(format.escape(), format.fast());
    }

    @Override
    protected String substring() {
        return formatString() + " " + (escaping ? "escaping" : "");
    }

    @Override
    void inited() {
        escaping = false;
        nextEscape = -1;
    }

    @Override
    void findSeps(long offset, long data, long endOffset) {
        int sep = sepFinder.next(data);
        int esc = escFinder.next(data);
        while (true) {
            int diff = sep - esc;
            if (diff == 0 || offset + sep > endOffset) {
                return;
            }
            if (diff < 0) {
                long position = offset + sep;
                handleSep(position);
                sep = sepFinder.next();
            } else {
                long position = offset + esc;
                handleEsc(position);
                esc = escFinder.next();
            }
        }
    }

    private void handleSep(long position) {
        boolean effective = !(escaping && position == nextEscape);
        if (effective) {
            markSeparator(position);
        } else {
            escaping = false;
        }
    }

    private void handleEsc(long position) {
        if (escaping && position == nextEscape) {
            escaping = false;
        } else {
            nextEscape = position + 1;
            escaping = true;
        }
    }
}
