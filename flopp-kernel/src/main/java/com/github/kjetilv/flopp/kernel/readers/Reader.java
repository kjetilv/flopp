package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;

import java.util.function.Consumer;

@FunctionalInterface
public interface Reader {

    void read(PartitionedSplitter splitter, Consumer<Columns> values);

    interface Columns {

        Column column(String name);

        default int columnNo(String name) {
            return column(name).colunmNo();
        }

        default Object get(String name) {
            return get(column(name).colunmNo());
        }

        Object get(int col);

        default LineSegment getRaw(String name) {
            return getRaw(column(name).colunmNo());
        }

        LineSegment getRaw(int col);

        default int getInt(String name) {
            return getInt(columnNo(name));
        }

        int getInt(int col);

        default long getLong(String name) {
            return getLong(columnNo(name));
        }

        long getLong(int col);

        default boolean getBoolean(String name) {
            return getBoolean(columnNo(name));
        }

        boolean getBoolean(int col);

        default short getShort(String name) {
            return getShort(columnNo(name));
        }

        short getShort(int col);

        default byte getByte(String name) {
            return getByte(columnNo(name));
        }

        byte getByte(int col);

        default char getChar(String name) {
            return getChar(columnNo(name));
        }

        char getChar(int col);

        default float getFloat(String name) {
            return getFloat(columnNo(name));
        }

        float getFloat(int col);

        default double getDouble(String name) {
            return getDouble(columnNo(name));
        }

        double getDouble(int col);
    }
}
