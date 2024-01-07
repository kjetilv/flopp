package com.github.kjetilv.flopp.kernel;

final class ByteSegPartitionSpliterator extends AbstractPartitionSpliterator<ByteSegPartitionSpliterator.ByteSeg> {

    private final ByteSeg byteSeg = new ByteSeg();

    ByteSegPartitionSpliterator(ByteSource byteSource, Partition partition, Shape shape, int bufferSize) {
        super(bufferSize, byteSource, partition, shape);
    }

    @Override
    protected ByteSeg item() {
        return byteSeg.with(lineBytes, 0, lineIndex);
    }

    public static final class ByteSeg {

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
