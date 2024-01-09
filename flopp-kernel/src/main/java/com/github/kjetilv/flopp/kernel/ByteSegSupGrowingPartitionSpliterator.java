package com.github.kjetilv.flopp.kernel;

import java.util.function.Supplier;

final class ByteSegSupGrowingPartitionSpliterator extends AbstractGrowingPartitionSpliterator<Supplier<ByteSeg>> {

    private final MutableByteSeg byteSeg = new MutableByteSeg();

    ByteSegSupGrowingPartitionSpliterator(ByteSource byteSource, Partition partition, Shape shape, int bufferSize) {
        super(bufferSize, byteSource, partition, shape);
    }

    @Override
    protected Supplier<ByteSeg> item() {
        return () -> byteSeg.with(lineBytes, 0, lineIndex);
    }
}
