package com.github.kjetilv.flopp.kernel;

public interface ByteSource {

    int fill(byte[] bytes, int offset, int length);
}
