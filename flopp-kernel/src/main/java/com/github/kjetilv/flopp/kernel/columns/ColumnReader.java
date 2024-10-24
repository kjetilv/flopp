package com.github.kjetilv.flopp.kernel.columns;

import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.util.function.Consumer;

@FunctionalInterface
public interface ColumnReader {

    void read(PartitionedSplitter splitter, Consumer<Columns> values);

    @SuppressWarnings("unused")
    interface Columns {

        default int col(String name) {
            return column(name).colunmNo();
        }

        default Object get(String name) {
            return get(column(name).colunmNo());
        }

        default LineSegment getRaw(String n) {
            return getRaw(column(n).colunmNo());
        }

        default int getInt(String n) {
            return getInt(col(n));
        }

        default long getLong(String n) {
            return getLong(col(n));
        }

        default boolean getBoolean(String n) {
            return getBoolean(col(n));
        }

        default short getShort(String n) {
            return getShort(col(n));
        }

        default byte getByte(String string) {
            return getByte(col(string));
        }

        default char getChar(String n) {
            return getChar(col(n));
        }

        default float getFloat(String n) {
            return getFloat(col(n));
        }

        default double getDouble(String n) {
            return getDouble(col(n));
        }

        Column column(String name);

        Object get(int c);

        LineSegment getRaw(int c);

        int getInt(int c);

        long getLong(int c);

        boolean getBoolean(int c);

        short getShort(int c);

        byte getByte(int c);

        char getChar(int c);

        float getFloat(int c);

        double getDouble(int c);
    }
}
