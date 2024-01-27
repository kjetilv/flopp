package com.github.kjetilv.flopp.kernel;

import java.util.function.Consumer;
import java.util.function.Function;

@FunctionalInterface
public interface Mediator<T> extends Function<Consumer<? super T>, Consumer<? super T>> {
}
