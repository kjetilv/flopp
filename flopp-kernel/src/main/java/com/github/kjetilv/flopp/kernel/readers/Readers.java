package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.Non;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.kjetilv.flopp.kernel.readers.Maps.map;

public final class Readers {

    public static Column column(String name, int columnNo) {
        return column(name, columnNo, null);
    }

    public static Column column(String name, int columnNo, Function<String, Object> parser) {
        return new Column(name, columnNo, parser);
    }

    public static Reader reader(Column... columns) {
        return (splitter, values) ->
            splitter.process(separatedLine ->
                map(
                    List.of(columns),
                    Column::name,
                    column ->
                        column.parse(separatedLine.column(column.colunmNo() - 1))
                ));
    }

    private Readers() {
    }

    public record Column(String name, int colunmNo, Function<String, Object> parser) {

        public Column {
            Non.negativeOrZero(colunmNo, "Columns are 1-indexed");
        }

        Object parse(String string) {
            return string == null || parser == null
                ? string
                : parser.apply(string);
        }
    }

    public interface Reader {

        void read(PartitionedSplitter splitter, Consumer<Map<String, Object>> values);
    }
}
