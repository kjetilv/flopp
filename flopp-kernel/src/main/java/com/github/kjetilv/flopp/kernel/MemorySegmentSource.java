package com.github.kjetilv.flopp.kernel;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import java.io.Closeable;
import java.lang.foreign.MemorySegment;
import java.util.Objects;

public interface MemorySegmentSource extends Closeable {

    Segment get();

    @Override
    default void close() {
    }

    record Segment(MemorySegment memorySegment, int shift) {

        public Segment {
            Objects.requireNonNull(memorySegment, "memorySegment");
            Non.negative(shift, "shift");
        }

        public long limit() {
            return memorySegment.byteSize() - SPECIES.length();
        }

        @Override
        public String toString() {
            return STR."\{getClass().getSimpleName()}[\{memorySegment}/\{shift}]";
        }
    }

    VectorSpecies<Byte> SPECIES = VectorShape.preferredShape().withLanes(ByteVector.SPECIES_PREFERRED.elementType());
}
