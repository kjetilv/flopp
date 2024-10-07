package com.github.kjetilv.flopp.kernel.readers;

import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.util.function.Consumer;

@FunctionalInterface
public interface ColumnReader {

    void read(PartitionedSplitter splitter, Consumer<Columns> values);

    @SuppressWarnings("unused")
    interface Columns {

        Column column(String name);

        default int col(String name) {
            return column(name).colunmNo();
        }

        default Object get(String name) {
            return get(column(name).colunmNo());
        }

        Object get(int c);

        default LineSegment getRaw(String n) {
            return getRaw(column(n).colunmNo());
        }

        LineSegment getRaw(int c);

        default int getInt(String n) {
            return getInt(col(n));
        }

        int getInt(int c);

        default long getLong(String n) {
            return getLong(col(n));
        }

        long getLong(int c);

        default boolean getBoolean(String n) {
            return getBoolean(col(n));
        }

        boolean getBoolean(int c);

        default short getShort(String n) {
            return getShort(col(n));
        }

        short getShort(int c);

        default byte getByte(String string) {
            return getByte(col(string));
        }

        byte getByte(int c);

        default char getChar(String n) {
            return getChar(col(n));
        }

        char getChar(int c);

        default float getFloat(String n) {
            return getFloat(col(n));
        }

        float getFloat(int c);

        default double getDouble(String n) {
            return getDouble(col(n));
        }

        double getDouble(int c);
    }
}
