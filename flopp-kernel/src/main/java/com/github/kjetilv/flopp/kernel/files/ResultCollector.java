package com.github.kjetilv.flopp.kernel.files;

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

    void sync(PartitionResult<T> partitionResult) {
        consumerSpliterator.accept(partitionResult);
    }

    void syncTo(Transfers<T> transfers) {
        results()
            .map(result ->
                transfer(transfers, result))
            .map(transfer ->
                CompletableFuture.runAsync(transfer, executorService))
            .toList()
            .forEach(CompletableFuture::join);
    }

    private Stream<PartitionResult<T>> results() {
        return StreamSupport.stream(consumerSpliterator, false);
    }

    private static <T> Transfer transfer(
        Transfers<T> transfers,
        PartitionResult<T> partitionResult
    ) {
        return transfers.transfer(partitionResult.partition(), partitionResult.result());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + consumerSpliterator + "]";
    }
}
