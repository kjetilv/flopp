package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.PartitionResult;

import java.util.concurrent.CompletableFuture;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class ResultCollector<T> {

    private final ResultConsumerSpliterator<T> consumerSpliterator;

    ResultCollector(int resultsCount, ToLongFunction<T> sizer) {
        this.consumerSpliterator = new ResultConsumerSpliterator<>(resultsCount, sizer);
    }

    public void collect(CompletableFuture<PartitionResult<T>> future) {
        consumerSpliterator.accept(future);
    }

    public Stream<PartitionResult<T>> streamCollected() {
        return StreamSupport.stream(consumerSpliterator, false);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + consumerSpliterator + "]";
    }
}
