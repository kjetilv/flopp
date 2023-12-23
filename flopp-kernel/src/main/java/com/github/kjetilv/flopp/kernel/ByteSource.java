package com.github.kjetilv.flopp.kernel;

@FunctionalInterface
public interface ByteSource {

    long fill(byte[] bytes, long offset, long length);
}
