package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.formats.Format;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedSplitters {

    Stream<PartitionedSplitter> splitters(Format format);

    Stream<CompletableFuture<PartitionedSplitter>> splitters(
        Format format,
        ExecutorService executorService
    );
}
