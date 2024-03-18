package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public interface Reader {

    static Column column(String name, int columnNo) {
        return column(name, columnNo, null);
    }

    static Column column(String name, int columnNo, Function<String, Object> parser) {
        return new Column(name, columnNo, parser);
    }

    static Reader of(Column... columns) {
        return (splitter, values) ->
            splitter.forEach(separatedLine ->
                values.accept(Maps.map(
                    Column::name,
                    column -> value(separatedLine, column),
                    columns
                )));
    }

    void read(PartitionedSplitter splitter, Consumer<Map<String, Object>> values);

    private static Object value(
        SeparatedLine separatedLine,
        Column column
    ) {
        return column.parse(separatedLine.column(column.colunmNo() - 1));
    }

    record Column(String name, int colunmNo, Function<String, Object> parser) {

        public Column {
            Non.negativeOrZero(colunmNo, "Columns are 1-indexed");
        }

        Object parse(String string) {
            return string == null || parser == null
                ? string
                : parser.apply(string);
        }
    }
}
