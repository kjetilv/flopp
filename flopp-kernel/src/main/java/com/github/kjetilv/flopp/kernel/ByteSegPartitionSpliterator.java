package com.github.kjetilv.flopp.kernel;

final class ByteSegPartitionSpliterator extends AbstractPartitionSpliterator<ByteSeg> {

    private final MutableByteSeg byteSeg = new MutableByteSeg();

    ByteSegPartitionSpliterator(ByteSource byteSource, Partition partition, Shape shape, int bufferSize) {
        super(bufferSize, byteSource, partition, shape);
    }

    @Override
    protected ByteSeg item() {
        return byteSeg.with(lineBytes, 0, lineIndex);
    }

    private static final class MutableByteSeg implements ByteSeg {

        private byte[] bytes;

        private int length;

        public byte[] bytes() {
            return bytes;
        }

        public int length() {
            return length;
        }

        public ByteSeg with(byte[] lineBytes, int i, int lineIndex) {
            this.bytes = lineBytes;
            this.length = lineIndex;
            return this;
        }
    }
}
