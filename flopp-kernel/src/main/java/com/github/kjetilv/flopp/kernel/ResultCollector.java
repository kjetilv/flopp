package com.github.kjetilv.flopp.kernel;

import java.util.concurrent.CompletableFuture;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class ResultCollector<T>  {

    private final ResultConsumerSpliterator<T> consumerSpliterator;

    ResultCollector(int resultsCount, ToIntFunction<T> sizer) {
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
