package com.github.kjetilv.flopp.kernel.bits;

public interface Origin {

    long ln();

    int col();

    default Origin immutable() {
        return new ImmutableOrigin(ln(), col());
    }
}
