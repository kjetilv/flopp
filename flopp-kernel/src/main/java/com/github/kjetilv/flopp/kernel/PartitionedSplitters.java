package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.formats.CsvFormat;
import com.github.kjetilv.flopp.kernel.formats.FwFormat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedSplitters {

    Stream<PartitionedSplitter> splitters(CsvFormat format);

    Stream<CompletableFuture<PartitionedSplitter>> splitters(CsvFormat format, ExecutorService executorService);

    Stream<PartitionedSplitter> splitters(FwFormat format);

    Stream<CompletableFuture<PartitionedSplitter>> splitters(FwFormat format, ExecutorService executorService);
}
