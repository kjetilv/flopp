package com.github.kjetilv.lopp;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PathResultConsumer implements Consumer<CompletableFuture<PartitionedFile.Result<Path>>> {

    private final ResultConsumerSpliterator spliterator;

    public PathResultConsumer(int resultsCount) {
        spliterator = new ResultConsumerSpliterator(resultsCount);
    }

    @Override
    public void accept(CompletableFuture<PartitionedFile.Result<Path>> resultCompletableFuture) {
        spliterator.accept(resultCompletableFuture);
    }

    public Stream<PartitionedFile.Result<Path>> stream() {
        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + spliterator + "]";
    }
}
