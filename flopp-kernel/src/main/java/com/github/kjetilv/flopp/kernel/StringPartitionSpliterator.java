package com.github.kjetilv.flopp.kernel;

final class StringPartitionSpliterator extends AbstractPartitionSpliterator<String> {

    StringPartitionSpliterator(ByteSource byteSource, Partition partition, Shape shape, int bufferSize) {
        super(bufferSize, byteSource, partition, shape);
    }

    @Override
    protected String item() {
        return new String(lineBytes, 0, lineIndex, charset);
    }

}
