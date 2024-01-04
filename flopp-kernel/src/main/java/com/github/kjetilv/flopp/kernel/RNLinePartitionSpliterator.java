package com.github.kjetilv.flopp.kernel;

final class RNLinePartitionSpliterator extends AbstractPartitionSpliterator<RNLine> {

    RNLinePartitionSpliterator(ByteSource byteSource, Partition partition, Shape shape, int bufferSize) {
        super(bufferSize, byteSource, partition, shape);
    }

    @Override
    protected RNLine item() {
        byte[] bytes = new byte[lineIndex];
        System.arraycopy(lineBytes, 0, bytes, 0, lineIndex);
        return new RNLine(bytes, partition.partitionNo(), nextLineNo);
    }
}
