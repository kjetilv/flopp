package com.github.kjetilv.flopp.kernel;

final class StringGrowingPartitionSpliterator extends AbstractGrowingPartitionSpliterator<String> {

    StringGrowingPartitionSpliterator(ByteSource byteSource, Partition partition, Shape shape, int bufferSize) {
        super(bufferSize, byteSource, partition, shape);
    }

    @Override
    protected String item() {
        return new String(lineBytes, 0, lineIndex, charset);
    }

}
