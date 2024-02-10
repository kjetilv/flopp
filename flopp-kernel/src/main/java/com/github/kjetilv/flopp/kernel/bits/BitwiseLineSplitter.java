package com.github.kjetilv.flopp.kernel.bits;

import java.util.function.Consumer;

@SuppressWarnings("unused")
public final class BitwiseLineSplitter implements Consumer<LineSegment> {

    private final BitwisePartitioned.Action action;

    private final long splitMask;

    private final long quoteMask;

    private final long escapeMask;

    private final int[] indexes;

    private final int endColumn;

    public BitwiseLineSplitter(
        char splitCharacter,
        BitwisePartitioned.Action action
    ) {
        this(
            splitCharacter,
            DEFAULT_QUOTE,
            DEFAULT_ESC,
            action,
            0,
            null
        );
    }

    public BitwiseLineSplitter(
        char splitCharacter,
        BitwisePartitioned.Action action,
        int endColumn
    ) {
        this(
            splitCharacter,
            DEFAULT_QUOTE,
            DEFAULT_ESC,
            action,
            endColumn,
            null
        );
    }

    public BitwiseLineSplitter(
        char splitCharacter,
        char quoteChar,
        char escapeChar,
        BitwisePartitioned.Action action
    ) {
        this(
            splitCharacter,
            quoteChar,
            escapeChar,
            action,
            0,
            null
        );
    }

    public BitwiseLineSplitter(
        char splitCharacter,
        char quoteChar,
        char escapeChar,
        BitwisePartitioned.Action action,
        int[] indexes
    ) {
        this(
            splitCharacter,
            quoteChar,
            escapeChar,
            action,
            0,
            indexes
        );
    }

    public BitwiseLineSplitter(
        char splitCharacter,
        char quoteChar,
        char escapeChar,
        BitwisePartitioned.Action action,
        int endColumn
    ) {
        this(
            splitCharacter,
            quoteChar,
            escapeChar,
            action,
            endColumn,
            null
        );
    }

    public BitwiseLineSplitter(
        char splitCharacter,
        BitwisePartitioned.Action action,
        int[] indexes
    ) {
        this(
            splitCharacter,
            DEFAULT_QUOTE,
            DEFAULT_ESC,
            action,
            0,
            indexes
        );
    }

    public BitwiseLineSplitter(
        char splitCharacter,
        char quoteChar,
        BitwisePartitioned.Action action,
        int[] indexes
    ) {
        this(
            splitCharacter,
            quoteChar,
            DEFAULT_ESC,
            action,
            0,
            indexes
        );
    }

    private BitwiseLineSplitter(
        char splitCharacter,
        char quoteChar,
        char escapeChar,
        BitwisePartitioned.Action action,
        int endColumn,
        int[] indexes
    ) {
        this.action = action;

        this.splitMask = createMask(splitCharacter);
        this.quoteMask = createMask(quoteChar);
        this.escapeMask = createMask(escapeChar);
        this.indexes = indexes == null ? NO_INDICES : indexes;
        this.endColumn = endColumn;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public void accept(LineSegment segment) {
        ReadState readState = new ReadState(
            segment,
            splitMask,
            quoteMask,
            escapeMask
        );
        ParseEvents events = new ParseEvents(
            (start, end) ->
                action.line(
                    segment.memorySegment(),
                    segment.startIndex() + start,
                    segment.startIndex() + end
                ),
            indexes,
            endColumn,
            Math.toIntExact(segment.length())
        );
        while (!readState.eventsDone(events)) {
        }
        if (!readState.done) {
            events.close();
        }
    }

    public static final char DEFAULT_QUOTE = '"';

    public static final char DEFAULT_ESC = '\\';

    public static final int[] NO_INDICES = new int[0];

    private static final int ALIGNMENT = 0x08;

    private static long createMask(char s) {
        long mask = s;
        for (int i = 0; i < ALIGNMENT; i++) {
            mask = (mask << ALIGNMENT) + s;
        }
        return mask;
    }

    private static final class ReadState {

        private final LineSegment lineSegment;

        private final long sepMaks;

        private final long quoMask;

        private final long escMasp;

        private final int lastOffset;

        private final int tail;

        private final int length;

        private int currentOffset;

        private boolean done;

        private ReadState(
            LineSegment lineSegment,
            long sepMaks,
            long quoMask,
            long escMasp
        ) {
            this.lineSegment = lineSegment;
            this.sepMaks = sepMaks;
            this.quoMask = quoMask;
            this.escMasp = escMasp;

            this.length = Math.toIntExact(lineSegment.length());
            this.tail = length % ALIGNMENT;
            this.lastOffset = length - tail;

        }

        private boolean eventsDone(Events events) {
            if (done) {
                return true;
            }
            long bytes = loadLong();

            long seps = mask(bytes, sepMaks);
            long quos = mask(bytes, quoMask);
            long escs = mask(bytes, escMasp);

            int nextSep = distance(seps);
            int nextQuo = distance(quos);
            int nextEsc = distance(escs);

            while (true) {
                switch (min(nextSep, nextQuo, nextEsc)) {
                    case SEP -> {
                        if (events.tokenAndDone(Token.SEP, currentOffset + nextSep)) {
                            return done = true;
                        }
                        seps &= CLEARED[nextSep];
                        nextSep = distance(seps);
                    }
                    case QUO -> {
                        if (events.tokenAndDone(Token.QUO, currentOffset + nextQuo)) {
                            return done = true;
                        }
                        quos &= CLEARED[nextQuo];
                        nextQuo = distance(quos);
                    }
                    case ESC -> {
                        if (events.tokenAndDone(Token.ESC, currentOffset + nextEsc)) {
                            return done = true;
                        }
                        escs &= CLEARED[nextEsc];
                        nextEsc = distance(escs);
                    }
                    case _I_ -> {
                        currentOffset += ALIGNMENT;
                        done = currentOffset >= length;
                        if (done) {
                            events.close();
                        }
                        return done;
                    }
                }
            }
        }

        private long loadLong() {
            if (currentOffset < lastOffset) {
                return lineSegment.longAt(currentOffset);
            }
            long l = 0;
            for (int i = tail - 1; i >= 0; i--) {
                byte b = lineSegment.byteAt(currentOffset + i);
                l = (l << Bits.ALIGNMENT) + b;
            }
            return l;
        }

        private static final long[] CLEARED = {
            0xFFFFFFFFFFFFFF00L,
            0xFFFFFFFFFFFF0000L,
            0xFFFFFFFFFF000000L,
            0xFFFFFFFF00000000L,
            0xFFFFFF0000000000L,
            0xFFFF000000000000L,
            0xFF00000000000000L,
            0x0000000000000000L
        };

        private static Token min(int sep, int quo, int esc) {
            int min = Math.min(Math.min(sep, quo), esc);
            if (min == ALIGNMENT) {
                return Token._I_;
            }
            if (min == sep) {
                return Token.SEP;
            }
            if (min == quo) {
                return Token.QUO;
            }
            return Token.ESC;
        }

        private static long mask(long bytes, long mask) {
            long masked = bytes ^ mask;
            long underflown = masked - 0x0101010101010101L;
            long clearedHighBits = underflown & ~masked;
            return clearedHighBits & 0x8080808080808080L;
        }

        private static int distance(long bytes) {
            return Long.numberOfTrailingZeros(bytes) / ALIGNMENT;
        }
    }

    private static final class ParseEvents implements Events {

        private boolean quoting;

        private boolean escaping;

        private int lastSplit;

        private final Line line;

        private final int[] indexes;

        private final int length;

        private final int endColumn;

        private int columnCount;

        private int indexedColumnCount;

        private final boolean emitAll;

        private ParseEvents(
            Line line,
            int[] indexes,
            int endColumn,
            int length
        ) {
            this.line = line;
            this.indexes = indexes;
            this.length = length;
            this.emitAll = endColumn <= 0 && indexes.length == 0;
            this.endColumn = endColumn;
        }

        @Override
        public boolean tokenAndDone(Token token, int pos) {
            switch (token) {
                case QUO -> {
                    if (escaping) {
                        escaping = false;
                    } else {
                        quoting = !quoting;
                    }
                }
                case ESC -> escaping = true;
                case SEP -> {
                    if (!quoting) {
                        if (escaping) {
                            escaping = false;
                        } else {
                            boolean done = lineAndDone(pos);
                            lastSplit = pos + 1;
                            return done;
                        }
                    }
                }
                case _I_ -> throw new IllegalStateException(STR."Should not get \{Token._I_} at \{pos}");
            }
            return false;
        }

        @Override
        public void close() {
            lineAndDone(length);
        }

        private boolean lineAndDone(int pos) {
            if (emitAll) {
                ship(pos);
                return false;
            }
            if (endColumn > 0) {
                if (columnCount < endColumn) {
                    ship(pos);
                }
                columnCount++;
                return columnCount == endColumn;
            }
            if (indexes[indexedColumnCount] == columnCount) {
                ship(pos);
                indexedColumnCount++;
            }
            columnCount++;
            return indexedColumnCount == indexes.length;
        }

        private void ship(int pos) {
            line.line(lastSplit, pos);
        }
    }

    private interface Line {

        void line(int start, int end);
    }

    private interface Events {

        boolean tokenAndDone(Token token, int pos);

        void close();
    }

    private enum Token {

        SEP,

        QUO,

        ESC,

        _I_
    }
}
