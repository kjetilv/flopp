package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.PartitionResult;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.Transfers;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class ResultCollector<T> {

    private final ResultConsumerSpliterator<T> consumerSpliterator;

    private final ExecutorService executorService;

    ResultCollector(int resultsCount, ToLongFunction<T> sizer, ExecutorService executorService) {
        this.consumerSpliterator = new ResultConsumerSpliterator<>(resultsCount, sizer);
        this.executorService = Objects.requireNonNull(executorService, "executorService");
    }

    public void sync(CompletableFuture<PartitionResult<T>> future) {
        consumerSpliterator.accept(future);
    }

    public Stream<PartitionResult<T>> streamCollected() {
        return StreamSupport.stream(consumerSpliterator, false);
    }

    public void sync(Transfers<T> transfers) {
        streamCollected()
            .map(result -> {
                System.out.println("Done: " + result);
                return transfers.transfer(result.partition(), result.result())
                    .in(executorService);
            })
            .toList()
            .forEach(CompletableFuture::join);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + consumerSpliterator + "]";
    }
}
