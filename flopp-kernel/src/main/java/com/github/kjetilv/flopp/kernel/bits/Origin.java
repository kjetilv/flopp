package com.github.kjetilv.flopp.kernel.bits;

public interface Origin {
    int file();

    long line();

    int column();
}
