package com.github.kjetilv.flopp.kernel;

public interface Range {

    long startIndex();

    long endIndex();

    default long length() {
        return endIndex() - startIndex();
    }
}
