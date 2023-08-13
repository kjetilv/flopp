package com.github.kjetilv.lopp;

import com.github.kjetilv.lopp.utils.Non;

import java.util.Objects;

public record NPLine(String line, long lineNo, int partition) {

    public static NPLine empty() {
        return NULL;
    }

    public NPLine(String line, long lineNo, int partition) {
        this.line = lineNo == Long.MAX_VALUE - 1 && partition == Integer.MAX_VALUE
            ? null
            : Objects.requireNonNull(line, "line");
        this.lineNo = Non.negative(lineNo, "lineNo") + 1;
        this.partition = Non.negative(partition, "partition");
    }

    private static final NPLine NULL = new NPLine(null, Long.MAX_VALUE - 1, Integer.MAX_VALUE);

    @Override
    public String toString() {
        if (line == null) {
            return "⎨NULL⎬";
        }
        String shortString = line.length() > 10 ? line.substring(0, 9) + "⋯" : line;
        return "⎨" + partition + "⨁" + lineNo + " `" + shortString + "`⎬";
    }
}
