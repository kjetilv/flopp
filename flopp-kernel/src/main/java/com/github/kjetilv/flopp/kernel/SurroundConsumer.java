package com.github.kjetilv.flopp.kernel;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

@FunctionalInterface
interface SurroundConsumer<T> extends BiConsumer<Consumer<T>, T> {
}
