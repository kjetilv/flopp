package com.github.kjetilv.flopp.kernel;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.Objects;

import static jdk.incubator.vector.VectorOperators.EQ;

public interface MemorySegmentSource  {

    Segment get();

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
            return charMask(offset, (byte) '\n');
        }

        public VectorMask<Byte> charMask(long offset, byte c) {
            ByteVector vector = vector(offset);
            return vector.compare(EQ, c);
        }

        private ByteVector vector(long offset) {
            try {
                return ByteVector.fromMemorySegment(SPECIES, memorySegment, offset, NATIVE_ORDER);
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
