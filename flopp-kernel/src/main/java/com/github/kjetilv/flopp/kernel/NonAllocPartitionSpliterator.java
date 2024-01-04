package com.github.kjetilv.flopp.kernel;

final class NonAllocPartitionSpliterator extends AbstractPartitionSpliterator<NonAllocPartitionSpliterator.ByteSeg> {

    NonAllocPartitionSpliterator(ByteSource byteSource, Partition partition, Shape shape, int bufferSize) {
        super(bufferSize, byteSource, partition, shape);
    }

    @Override
    protected ByteSeg item() {
        return new ByteSeg(lineBytes, 0, lineIndex);
    }

    public record ByteSeg(byte[] bytes, int offset, int length) {}
}
