package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.formats.FlatFileFormat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedSplitters {

    Stream<PartitionedSplitter> splitters(FlatFileFormat format);

    Stream<CompletableFuture<PartitionedSplitter>> splitters(FlatFileFormat format, ExecutorService executorService);
}
