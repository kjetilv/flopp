package com.github.kjetilv.flopp.kernel.bits;

import java.util.function.Function;

@FunctionalInterface
public interface ActionMediator
    extends Function<BitwisePartitionHandler.Action, BitwisePartitionHandler.Action> {
}
