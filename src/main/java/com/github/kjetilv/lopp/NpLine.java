package com.github.kjetilv.lopp;

import java.util.Objects;

public record NpLine(String line, int partition, long lineNo) {

    public static NpLine empty() {
        return NULL;
    }

    public NpLine(String line, int partition, long lineNo) {
        this.line = lineNo == Long.MAX_VALUE - 1 && partition == Integer.MAX_VALUE
            ? null
            : Objects.requireNonNull(line, "line");
        this.lineNo = Non.negative(lineNo, "lineNo") + 1;
        this.partition = Non.negative(partition, "partition");
    }

    private static final NpLine NULL = new NpLine(null, Integer.MAX_VALUE, Long.MAX_VALUE - 1);

    @Override
    public String toString() {
        if (line == null) {
            return "⎨NULL⎬";
        }
        int length = line.length();
        String shortString = length > 10 ? line.substring(0, 6) + "⋯" + line.substring(length - 3) : line;
        return "⎨" + partition + "⎯" + lineNo + ": `" + shortString + "`⎬";
    }
}
