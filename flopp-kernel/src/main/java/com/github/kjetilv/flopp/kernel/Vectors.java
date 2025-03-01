package com.github.kjetilv.flopp.kernel;

import java.util.function.LongSupplier;

public final class Vectors {

    private Vectors() {
    }

    public interface ByteFinder extends LongSupplier {

        @Override
        default long getAsLong() {
            return next();
        }

        long next();
    }
}
