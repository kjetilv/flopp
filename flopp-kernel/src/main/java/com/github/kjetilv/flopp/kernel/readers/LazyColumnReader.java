package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;
import com.github.kjetilv.flopp.kernel.util.Maps;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.github.kjetilv.flopp.kernel.readers.Column.Parser.*;

final class LazyColumnReader implements ColumnReader, ColumnReader.Columns {

    static ColumnReader create(String header, Format.Csv format) {
        return create(discoverColumns(header, format));
    }

    static ColumnReader create(List<Column> columns) {
        return new LazyColumnReader(columns);
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

    private LazyColumnReader(List<Column> columnsList) {
        if (columnsList == null || columnsList.isEmpty()) {
            throw new IllegalStateException("No columns!");
        }
        this.columnMap = columnMap(columnsList);
        int size = maxColumnNo(columnsList) + 1;
        this.columns = (Column[]) Array.newInstance(Column.class, size);
        columnsList.forEach(column ->
            this.columns[column.colunmNo()] = column);

        Obj[] objs = null;
        I[] is = null;
        L[] ls = null;
        Bo[] bos = null;
        By[] bys = null;
        C[] cs = null;
        S[] shs = null;
        F[] fs = null;
        D[] ds = null;

        this.columnMap.forEach((_, column) ->
            this.columns[column.colunmNo()] = column);
        int[] columnNos = columnsList.stream()
            .mapToInt(Column::colunmNo)
            .sorted()
            .toArray();

        for (int i : columnNos) {
            switch (this.columns[i].parser()) {
                case Obj obj -> (objs == null ? objs = new Obj[size] : objs)[i] = obj;
                case Bo bo -> (bos == null ? bos = new Bo[size] : bos)[i] = bo;
                case By by -> (bys == null ? bys = new By[size] : bys)[i] = by;
                case C c -> (cs == null ? cs = new C[size] : cs)[i] = c;
                case D d -> (ds == null ? ds = new D[size] : ds)[i] = d;
                case F f -> (fs == null ? fs = new F[size] : fs)[i] = f;
                case I ing -> (is == null ? is = new I[size] : is)[i] = ing;
                case L l -> (ls == null ? ls = new L[size] : ls)[i] = l;
                case S s -> (shs == null ? shs = new S[size] : shs)[i] = s;
            }
        }

        this.objs = objs;
        this.is = is;
        this.ls = ls;
        this.bos = bos;
        this.bys = bys;
        this.cs = cs;
        this.shs = shs;
        this.fs = fs;
        this.ds = ds;
    }

    @Override
    public void read(PartitionedSplitter splitter, Consumer<Columns> values) {
        splitter.forEach(separatedLine ->
            values.accept(columns(separatedLine)));
    }

    @Override
    public Column column(String name) {
        return columnMap.get(name);
    }

    @Override
    public Object get(int c) {
        return objs[c].parse(sl, c);
    }

    @Override
    public LineSegment getRaw(int c) {
        return sl.segment(c);
    }

    @Override
    public int getInt(int c) {
        return is[c].parse(sl, c);
    }

    @Override
    public long getLong(int c) {
        return ls[c].parse(sl, c);
    }

    @Override
    public boolean getBoolean(int c) {
        return bos[c].parse(sl, c);
    }

    @Override
    public short getShort(int c) {
        return shs[c].parse(sl, c);
    }

    @Override
    public byte getByte(int c) {
        return bys[c].parse(sl, c);
    }

    @Override
    public char getChar(int c) {
        return cs[c].parse(sl, c);
    }

    @Override
    public float getFloat(int c) {
        return fs[c].parse(sl, c);
    }

    @Override
    public double getDouble(int c) {
        return ds[c].parse(sl, c);
    }

    private Columns columns(SeparatedLine separatedLine) {
        this.sl = separatedLine;
        return this;
    }

    private static int maxColumnNo(List<Column> columns) {
        return columns.stream().mapToInt(Column::colunmNo)
            .max()
            .orElseThrow(() ->
                new IllegalStateException("Unepxected missing max"));
    }

    private static List<Column> discoverColumns(String header, Format.Csv format) {
        String[] headers = format.split(header);
        return IntStream.range(0, headers.length)
            .mapToObj(i ->
                Column.ofString(headers[i], i))
            .toList();
    }

    private static Map<String, Column> columnMap(List<Column> columns) {
        Map<String, Column> map = new HashMap<>(Maps.mapCapacity(columns.size()));
        for (Column column : columns) {
            String name = column.name();
            if (name != null) {
                Column existing = map.put(name, column);
                if (existing != null) {
                    throw new IllegalArgumentException("Non-unique column name: " + name);
                }
            }
        }
        return Map.copyOf(map);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + sl + "]";
    }
}
