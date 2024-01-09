package com.github.kjetilv.flopp.kernel;

@FunctionalInterface
public interface ByteSource {

    int fill(byte[] bytes);
}
