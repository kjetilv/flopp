package com.github.kjetilv.lopp;

public interface ByteSource {

    int fill(byte[] bytes, int offset, int length);
}
