package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.Non;

import java.util.function.Function;

public record Column(String name, int colunmNo, Function<String, Object> parser) {

    public Column {
        Non.negativeOrZero(colunmNo, "Columns are 1-indexed, first column is 1");
    }

    Object parse(String string) {
        return string == null || parser == null
            ? string
            : parser.apply(string);
    }
}
