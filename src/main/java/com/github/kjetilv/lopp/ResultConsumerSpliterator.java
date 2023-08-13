package com.github.kjetilv.lopp;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class ResultConsumerSpliterator extends AbstractResultSpliterator
    implements Consumer<CompletableFuture<PartitionedFile.Result<Path>>> {

    ResultConsumerSpliterator(int resultsCount) {
        super(resultsCount);
    }

    @Override
    public void accept(CompletableFuture<PartitionedFile.Result<Path>> future) {
        future.whenComplete(this::recordCompletion);
    }
}
