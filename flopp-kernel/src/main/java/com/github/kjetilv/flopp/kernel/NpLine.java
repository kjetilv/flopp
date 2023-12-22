package com.github.kjetilv.flopp.kernel;

import java.util.Objects;

public record NpLine(String line, int partition, long lineNo) {

    public static NpLine empty() {
        return NULL;
    }

    public NpLine {
        if (lineNo < 1) {
            throw new IllegalArgumentException("Line numbers are one-indexed: " + lineNo);
        }
    }

    public NpLine(String line, int partition, long lineNo, boolean isNull) {
        this(
            isNull ? null : Objects.requireNonNull(line, "line"),
            Non.negative(partition, "partition"),
            Non.negativeOrZero(lineNo, "lineNo")
        );
    }

    @Override
    public String toString() {
        if (line == null) {
            return NULL_STRING;
        }
        int length = line.length();
        return length <= 12
            ? STRING_FORMAT_SHORT.formatted(partition, lineNo, line)
            : STRING_FORMAT.formatted(
                partition,
                lineNo,
                line.substring(0, 8),
                line.substring(length - 3)
            );
    }

    private static final NpLine NULL =
        new NpLine(null, Integer.MAX_VALUE, Long.MAX_VALUE, true);

    private static final String NULL_STRING = "⎨NULL⎬";

    private static final String STRING_FORMAT = "⎨%d⎯%d: `%s⋯%s`⎬";

    private static final String STRING_FORMAT_SHORT = "⎨%d⎯%d: `%s`⎬";
}
