package com.github.kjetilv.flopp.kernel;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import java.io.Closeable;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.Objects;

import static jdk.incubator.vector.VectorOperators.EQ;

public interface MemorySegmentSource extends Closeable {

    Segment get();

    @Override
    default void close() {
    }

    @SuppressWarnings("MethodMayBeStatic")
    record Segment(MemorySegment memorySegment, int shift) {

        public Segment {
            Objects.requireNonNull(memorySegment, "memorySegment");
            Non.negative(shift, "shift");
        }

        public long maxReadOffset() {
            return memorySegment.byteSize() - SPECIES.length();
        }

        public VectorSpecies<Byte> species() {
            return SPECIES;
        }

        public VectorMask<Byte> lineMask(long offset) {
            return charMask(offset, '\n');
        }

        public VectorMask<Byte> charMask(long offset, char c) {
            return vector(offset).compare(EQ, c);
        }

        private ByteVector vector(long offset) {
            try {
                return ByteVector.fromMemorySegment(species(), memorySegment(), offset, NATIVE_ORDER);
            } catch (Exception e) {
                throw new IllegalStateException(
                    STR."\{this} failed to open vector @ \{offset}", e);
            }
        }

        private static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();

        @Override
        public String toString() {
            return STR."\{getClass().getSimpleName()}[\{memorySegment}/\{shift}]";
        }
    }

    VectorSpecies<Byte> SPECIES = VectorShape.preferredShape().withLanes(ByteVector.SPECIES_PREFERRED.elementType());
}
