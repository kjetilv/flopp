package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.formats.CsvFormat;
import com.github.kjetilv.flopp.kernel.util.Maps;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.github.kjetilv.flopp.kernel.readers.Column.Parser.*;

final class LazyReader implements Reader, Reader.Columns {

    static Reader create(List<Column> columns) {
        return new LazyReader(columns);
    }

    static Reader create(String header, CsvFormat format) {
        List<Column> columns = discoverColumns(header, format);
        return new LazyReader(columns);
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

    private LazyReader(List<Column> columnsList) {
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
    public String toString() {
        return getClass().getSimpleName() + "[" + sl + "]";
    }

    private static int maxColumnNo(List<Column> columns) {
        return columns.stream().mapToInt(Column::colunmNo)
            .max()
            .orElseThrow(() ->
                new IllegalStateException("Unepxected missing max"));
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
    public Object get(int col) {
        Obj obj = objs[col];
        return obj.parse(sl, col);
    }

    @Override
    public LineSegment getRaw(int col) {
        return sl.segment(col);
    }

    @Override
    public int getInt(int col) {
        I i = is[col];
        return i.parse(sl, col);
    }

    @Override
    public long getLong(int col) {
        L l = ls[col];
        return l.parse(sl, col);
    }

    @Override
    public boolean getBoolean(int col) {
        Bo bo = bos[col];
        return bo.parse(sl, col);
    }

    @Override
    public short getShort(int col) {
        S sh = shs[col];
        return sh.parse(sl, col);
    }

    @Override
    public byte getByte(int col) {
        By by = bys[col];
        return by.parse(sl, col);
    }

    @Override
    public char getChar(int col) {
        C c = cs[col];
        return c.parse(sl, col);
    }

    @Override
    public float getFloat(int col) {
        F f = fs[col];
        return f.parse(sl, col);
    }

    @Override
    public double getDouble(int col) {
        D[] d = ds;
        return d[col].parse(sl, col);
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
}
