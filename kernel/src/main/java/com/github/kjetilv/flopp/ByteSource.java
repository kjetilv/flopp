package com.github.kjetilv.flopp;

public interface ByteSource {

    int fill(byte[] bytes, int offset, int length);
}
