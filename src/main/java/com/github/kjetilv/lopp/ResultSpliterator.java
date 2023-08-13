package com.github.kjetilv.lopp;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class ResultSpliterator extends AbstractResultSpliterator {

    ResultSpliterator(List<CompletableFuture<PartitionedFile.Result<Path>>> results) {
        super(results.size());
        results.forEach(resultCompletableFuture ->
                resultCompletableFuture.whenComplete(this::recordCompletion));
    }
}
