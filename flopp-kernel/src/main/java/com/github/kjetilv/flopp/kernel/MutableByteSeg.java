package com.github.kjetilv.flopp.kernel;

final class MutableByteSeg implements ByteSeg {

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
