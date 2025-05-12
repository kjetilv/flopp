package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.util.MemorySegmentByteFinder;

import java.lang.foreign.MemorySegment;
import java.util.function.LongSupplier;

@SuppressWarnings("unused")
public final class Vectors {

    public static Finder finder(MemorySegment memorySegment, char b) {
        return finder(memorySegment, (byte) b);
    }

    public static Finder finder(MemorySegment memorySegment, byte b) {
        return finder(memorySegment, 0L, b);
    }

    public static Finder finder(MemorySegment memorySegment, long offset, char b) {
        return finder(memorySegment, offset, (byte) b);
    }

    public static Finder finder(MemorySegment memorySegment, long offset, byte b) {
        return new MemorySegmentByteFinder(memorySegment, offset, b);
    }

    private Vectors() {
    }

    public interface Finder extends LongSupplier {

        @Override
        default long getAsLong() {
            return next();
        }

        long next();
    }
}
