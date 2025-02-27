package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Bits;
import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.function.Consumer;

final class BitwiseCsvEscapeSplitter extends AbstractBitwiseCsvLineSplitter {

    private final Bits.Finder escFinder;

    private boolean escaping;

    private long nextEscape;

    BitwiseCsvEscapeSplitter(Consumer<SeparatedLine> lines, Format.Csv.Escape format) {
        super(lines, format);
        this.escFinder = Bits.finder(format.escape(), format.fast());
    }

    @Override
    protected String substring() {
        return formatString() + " " + (escaping ? "escaping" : "");
    }

    @Override
    protected void inited() {
        escaping = false;
        nextEscape = -1;
    }

    @Override
    protected void findSeps(long offset, long data, long endOffset) {
        int sep = nextSep(data);
        int esc = escFinder.next(data);
        while (true) {
            int diff = sep - esc;
            if (diff == 0 || offset + sep > endOffset) {
                return;
            }
            if (diff < 0) {
                handleSep(offset + sep);
                sep = nextSep();
            } else {
                handleEsc(offset + esc);
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
