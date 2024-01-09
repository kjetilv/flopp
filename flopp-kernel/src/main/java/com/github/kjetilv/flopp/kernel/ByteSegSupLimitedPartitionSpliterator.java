package com.github.kjetilv.flopp.kernel;

import java.util.function.Supplier;

final class ByteSegSupLimitedPartitionSpliterator extends AbstractLimitedPartitionSpliterator<Supplier<ByteSeg>> {

    private final MutableByteSeg byteSeg = new MutableByteSeg();

    ByteSegSupLimitedPartitionSpliterator(ByteSource byteSource, Partition partition, Shape shape, int bufferSize) {
        super(bufferSize, byteSource, partition, shape);
    }

    @Override
    protected Supplier<ByteSeg> item() {
        return () -> byteSeg.with(lineBytes, 0, lineIndex);
    }
}
