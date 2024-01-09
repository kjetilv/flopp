package com.github.kjetilv.flopp.kernel;

final class BytesLimitedPartitionSpliterator extends AbstractLimitedPartitionSpliterator<byte[]> {

    BytesLimitedPartitionSpliterator(ByteSource byteSource, Partition partition, Shape shape, int bufferSize) {
        super(bufferSize, byteSource, partition, shape);
    }

    @Override
    protected byte[] item() {
        byte[] bytes = new byte[lineIndex];
        System.arraycopy(lineBytes, 0, bytes, 0, lineIndex);
        return bytes;
    }
}
