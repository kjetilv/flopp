package com.github.kjetilv.flopp.kernel.readers;

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

        Object get(int columnNo);

        default int getInt(String name) {
            return getInt(columnNo(name));
        }

        int getInt(int columnNo);

        default long getLong(String name) {
            return getLong(columnNo(name));
        }

        long getLong(int columnNo);

        default boolean getBoolean(String name) {
            return getBoolean(columnNo(name));
        }

        boolean getBoolean(int columnNo);

        default short getShort(String name) {
            return getShort(columnNo(name));
        }

        short getShort(int columnNo);

        default byte getByte(String name) {
            return getByte(columnNo(name));
        }

        byte getByte(int columnNo);

        default char getChar(String name) {
            return getChar(columnNo(name));
        }

        char getChar(int columnNo);

        default float getFloat(String name) {
            return getFloat(columnNo(name));
        }

        float getFloat(int columnNo);

        default double getDouble(String name) {
            return getDouble(columnNo(name));
        }

        double getDouble(int columnNo);
    }
}
