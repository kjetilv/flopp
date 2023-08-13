package com.github.kjetilv.flopp.kernel.util;

import java.text.NumberFormat;
import java.util.Locale;

public final class Print {

    public static String bigInt(long i) {
        return i < 10_000
            ? String.valueOf(i)
            : NUMBER_INSTANCE.format(i);
    }

    private Print(){
    }

    private static final NumberFormat NUMBER_INSTANCE =
        NumberFormat.getNumberInstance(Locale.ROOT);
}
