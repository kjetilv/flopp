package com.github.kjetilv.lopp;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PathResultCollector {

    private final List<CompletableFuture<PartitionedFile.Result<Path>>> results;

    public PathResultCollector(List<CompletableFuture<PartitionedFile.Result<Path>>> results) {
        this.results = Objects.requireNonNull(results, "results");
    }

    public Stream<PartitionedFile.Result<Path>> stream() {
        return StreamSupport.stream(new ResultSpliterator(results), false);
    }
}
