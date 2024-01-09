package com.github.kjetilv.flopp.kernel;

final class NLineLimitedPartitionSpliterator extends AbstractLimitedPartitionSpliterator<NLine> {

    NLineLimitedPartitionSpliterator(ByteSource byteSource, Partition partition, Shape shape, int bufferSize) {
        super(bufferSize, byteSource, partition, shape);
    }

    @Override
    protected NLine item() {
        String line = new String(lineBytes, 0, lineIndex, charset);
        return new NLine(line, partition.partitionNo(), nextLineNo);
    }
}
