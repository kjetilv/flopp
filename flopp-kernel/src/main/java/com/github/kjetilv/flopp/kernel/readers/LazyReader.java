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

    private final Column.Parser.Obj[] objs;

    private final Column.Parser.Ing[] ings;

    private final Column.Parser.Lon[] lons;

    private final Column.Parser.Boo[] boos;

    private final Column.Parser.Byt[] byts;

    private final Column.Parser.Cha[] chas;

    private final Column.Parser.Sho[] shos;

    private final Column.Parser.Flo[] flos;

    private final Column.Parser.Dou[] dous;

    private SeparatedLine separatedLine;

    private LazyReader(Map<String, Column> columnMap) {
        this.columnMap = Map.copyOf(columnMap);
        int maxCol = columnMap.values()
            .stream()
            .max(Comparator.comparingInt(Column::colunmNo))
            .map(Column::colunmNo)
            .orElseThrow(() ->
                new IllegalStateException("No max column: " + columnMap));

        int size = maxCol + 1;

        this.objs = new Column.Parser.Obj[size];
        this.ings = new Column.Parser.Ing[size];
        this.lons = new Column.Parser.Lon[size];
        this.boos = new Column.Parser.Boo[size];
        this.byts = new Column.Parser.Byt[size];
        this.chas = new Column.Parser.Cha[size];
        this.shos = new Column.Parser.Sho[size];
        this.flos = new Column.Parser.Flo[size];
        this.dous = new Column.Parser.Dou[size];

        this.columns = new Column[size];
        this.columnMap.forEach((_, column) ->
            columns[column.colunmNo()] = column);
        int[] columnNos = this.columnMap.values()
            .stream().mapToInt(Column::colunmNo).sorted().toArray();

        for (int columnNo : columnNos) {
            switch (columns[columnNo].parser()) {
                case Column.Parser.Obj obj -> objs[columnNo] = obj;
                case Column.Parser.Boo boo -> boos[columnNo] = boo;
                case Column.Parser.Byt byt -> byts[columnNo] = byt;
                case Column.Parser.Cha cha -> chas[columnNo] = cha;
                case Column.Parser.Dou dou -> dous[columnNo] = dou;
                case Column.Parser.Flo flo -> flos[columnNo] = flo;
                case Column.Parser.Ing ing -> ings[columnNo] = ing;
                case Column.Parser.Lon lon -> lons[columnNo] = lon;
                case Column.Parser.Sho sho -> shos[columnNo] = sho;
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
    public Object get(int columnNo) {
        return objs[columnNo].parse(separatedLine, columnNo);
    }

    @Override
    public int getInt(int columnNo) {
        return ings[columnNo].parse(separatedLine, columnNo);
    }

    @Override
    public long getLong(int columnNo) {
        return lons[columnNo].parse(separatedLine, columnNo);
    }

    @Override
    public boolean getBoolean(int columnNo) {
        return boos[columnNo].parse(separatedLine, columnNo);
    }

    @Override
    public short getShort(int columnNo) {
        return shos[columnNo].parse(separatedLine, columnNo);
    }

    @Override
    public byte getByte(int columnNo) {
        return byts[columnNo].parse(separatedLine, columnNo);
    }

    @Override
    public char getChar(int columnNo) {
        return chas[columnNo].parse(separatedLine, columnNo);
    }

    @Override
    public float getFloat(int columnNo) {
        return flos[columnNo].parse(separatedLine, columnNo);
    }

    @Override
    public double getDouble(int columnNo) {
        return dous[columnNo].parse(separatedLine, columnNo);
    }

    private Columns columns(SeparatedLine separatedLine) {
        this.separatedLine = separatedLine;
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
