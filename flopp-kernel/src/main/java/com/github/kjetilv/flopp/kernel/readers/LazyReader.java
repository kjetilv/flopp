package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.CsvFormat;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.util.Maps;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.github.kjetilv.flopp.kernel.readers.Column.Parser.*;

final class LazyReader implements Reader, Reader.Columns {

    static Reader create(List<Column> columns) {
        return readerFor(columnMap(columns));
    }

    static Reader create(String header, CsvFormat format) {
        List<Column> columns = discoverColumns(header, format);
        return readerFor(columnMap(columns));
    }

    static Reader readerFor(Map<String, Column> columnMap) {
        return new LazyReader(columnMap);
    }

    private final Map<String, Column> columnMap;

    private final Column[] columns;

    private final Obj[] objs;

    private final I[] is;

    private final L[] ls;

    private final Bo[] bos;

    private final By[] bys;

    private final C[] cs;

    private final S[] shs;

    private final F[] fs;

    private final D[] ds;

    private SeparatedLine sl;

    private LazyReader(Map<String, Column> columnMap) {
        this.columnMap = Map.copyOf(columnMap);
        int maxCol = columnMap.values()
            .stream()
            .max(Comparator.comparingInt(Column::colunmNo))
            .map(Column::colunmNo)
            .orElseThrow(() ->
                new IllegalStateException("No max column: " + columnMap));

        int size = maxCol + 1;

        this.objs = new Obj[size];
        this.is = new I[size];
        this.ls = new L[size];
        this.bos = new Bo[size];
        this.bys = new By[size];
        this.cs = new C[size];
        this.shs = new S[size];
        this.fs = new F[size];
        this.ds = new D[size];

        this.columns = new Column[size];
        this.columnMap.forEach((_, column) ->
            columns[column.colunmNo()] = column);
        int[] columnNos = this.columnMap.values()
            .stream().mapToInt(Column::colunmNo).sorted().toArray();

        for (int i : columnNos) {
            switch (columns[i].parser()) {
                case Obj obj -> objs[i] = obj;
                case Bo boo -> bos[i] = boo;
                case By by -> bys[i] = by;
                case C c -> cs[i] = c;
                case D d -> ds[i] = d;
                case F f -> fs[i] = f;
                case I ing -> is[i] = ing;
                case L l -> ls[i] = l;
                case S s -> this.shs[i] = s;
            }
        }
    }

    @Override
    public void read(PartitionedSplitter splitter, Consumer<Columns> values) {
        splitter.forEach(separatedLine ->
            values.accept(
                columns(separatedLine)));
    }

    @Override
    public Column column(String name) {
        return columnMap.get(name);
    }

    @Override
    public Object get(int col) {
        return objs[col].parse(sl, col);
    }

    @Override
    public int getInt(int col) {
        return is[col].parse(sl, col);
    }

    @Override
    public long getLong(int col) {
        return ls[col].parse(sl, col);
    }

    @Override
    public boolean getBoolean(int col) {
        return bos[col].parse(sl, col);
    }

    @Override
    public short getShort(int col) {
        return shs[col].parse(sl, col);
    }

    @Override
    public byte getByte(int col) {
        return bys[col].parse(sl, col);
    }

    @Override
    public char getChar(int col) {
        return cs[col].parse(sl, col);
    }

    @Override
    public float getFloat(int col) {
        return fs[col].parse(sl, col);
    }

    @Override
    public double getDouble(int col) {
        return ds[col].parse(sl, col);
    }

    private Columns columns(SeparatedLine separatedLine) {
        this.sl = separatedLine;
        return this;
    }

    private static List<Column> discoverColumns(String header, CsvFormat format) {
        String[] headers = format.split(header);
        return IntStream.range(0, headers.length)
            .mapToObj(i ->
                Column.ofString(headers[i], i))
            .toList();
    }

    private static Map<String, Column> columnMap(List<Column> columns) {
        Map<String, Column> map = Maps.ofSize(columns.size());
        for (Column column : columns) {
            map.put(column.name(), column);
        }
        return Map.copyOf(map);
    }
}
