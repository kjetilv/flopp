package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.formats.FlatFileFormat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedSplitters {

    <F extends FlatFileFormat<F>> Stream<PartitionedSplitter> splitters(F format);

    <F extends FlatFileFormat<F>>  Stream<CompletableFuture<PartitionedSplitter>> splitters(F format, ExecutorService executorService);
}
