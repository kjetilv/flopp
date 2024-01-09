package com.github.kjetilv.flopp.kernel;

final class ByteSegLimitedPartitionSpliterator extends AbstractLimitedPartitionSpliterator<ByteSeg> {

    private final MutableByteSeg byteSeg = new MutableByteSeg();

    ByteSegLimitedPartitionSpliterator(ByteSource byteSource, Partition partition, Shape shape, int bufferSize) {
        super(bufferSize, byteSource, partition, shape);
    }

    @Override
    protected ByteSeg item() {
        return byteSeg.with(lineBytes, 0, lineIndex);
    }
}
