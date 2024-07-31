package com.github.kjetilv.flopp.kernel.util;

import java.util.Objects;
import java.util.function.BinaryOperator;

public final class Combine {

    public static <T> BinaryOperator<T> none() {
        return Combine::noMerge;
    }

    public static <T> BinaryOperator<T> same() {
        return (t1, t2) ->
            Objects.equals(t1, t2)
                ? t1
                : noMerge(t1, t2);
    }

    public static <T> BinaryOperator<T> either() {
        return (t, _) -> t;
    }

    private Combine() {
    }

    private static <T> T noMerge(T t1, T t2) {
        throw new IllegalStateException("Cannot merge: " + t1 + ", " + t2);
    }
}
