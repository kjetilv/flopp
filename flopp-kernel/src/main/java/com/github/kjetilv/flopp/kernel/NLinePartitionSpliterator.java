package com.github.kjetilv.flopp.kernel;

final class NLinePartitionSpliterator extends AbstractPartitionSpliterator<NLine> {

    NLinePartitionSpliterator(ByteSource byteSource, Partition partition, Shape shape, int bufferSize) {
        super(bufferSize, byteSource, partition, shape);
    }

    @Override
    protected NLine item() {
        String line = new String(lineBytes, 0, lineIndex, charset);
        return new NLine(line, partition.partitionNo(), nextLineNo);
    }
}
