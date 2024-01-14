package com.github.kjetilv.flopp.kernel;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

@FunctionalInterface
public interface SurroundConsumer<T> extends BiConsumer<Consumer<? super T>, T> {
}
