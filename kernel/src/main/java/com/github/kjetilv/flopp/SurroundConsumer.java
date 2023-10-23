package com.github.kjetilv.flopp;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

interface SurroundConsumer<T> extends BiConsumer<Consumer<T>, T> {
}
