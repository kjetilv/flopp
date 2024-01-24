package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.LineSegment;

import java.io.Closeable;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface PartitionedStreams extends Closeable {

    Stream<PartitionStreamer> streamers();

    Stream<VectorPartitionStreamer> vectorStreamers();

    @Override
    default void close() {
    }

    interface VectorPartitionStreamer extends Closeable {

        Partition partition();

        Stream<LineSegment> memorySegments();

        @Override
        default void close() {
        }
    }

    interface PartitionStreamer extends Closeable {

        Partition partition();

        Stream<String> lines();

        Stream<byte[]> rawLines();

        Stream<NLine> nLines();

        Stream<RNLine> rnLines();

        Stream<ByteSeg> byteSegs();

        Stream<Supplier<ByteSeg>> suppliedByteSegs();

        @Override
        default void close() {
        }
    }
}
