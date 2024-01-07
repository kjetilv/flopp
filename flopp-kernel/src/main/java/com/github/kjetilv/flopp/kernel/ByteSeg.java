package com.github.kjetilv.flopp.kernel;

public interface ByteSeg {

    byte[] bytes();

    int length();

    default int offset() {
        return 0;
    }
}
