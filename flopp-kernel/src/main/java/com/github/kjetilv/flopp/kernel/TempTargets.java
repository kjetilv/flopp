package com.github.kjetilv.flopp.kernel;

@FunctionalInterface
public interface TempTargets<T> {

    T temp(Partition partition);
}
