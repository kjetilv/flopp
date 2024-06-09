package com.github.kjetilv.flopp.kernel.util;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

public final class Print {

    public static String bigInt(long i) {
        return NUMBER_INSTANCE.format(i);
    }

    private Print(){
    }

    private static final NumberFormat NUMBER_INSTANCE =
        NumberFormat.getNumberInstance(Locale.ROOT);
}
