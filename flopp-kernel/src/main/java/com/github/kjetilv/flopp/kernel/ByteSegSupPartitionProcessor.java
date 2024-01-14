package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

class ByteSegSupPartitionProcessor<P> extends AbstractPartitionProcessor<P, Supplier<ByteSeg>> {

    ByteSegSupPartitionProcessor(
        PartitionedMapper partitionedMapper,
        Charset charset,
        int partitionCount,
        LinesWriterFactory<P> linesWriterFactory,
        TempTargets<P> tempTargets,
        ToIntFunction<P> sizer,
        Transfers<P> transfers,
        ExecutorService executorService
    ) {
        super(
            partitionedMapper,
            charset,
            partitionCount,
            linesWriterFactory,
            tempTargets,
            sizer,
            transfers,
            executorService
        );
    }

    @Override
    protected Stream<CompletableFuture<PartitionResult<P>>> futures(
        BiFunction<Partition, Stream<Supplier<ByteSeg>>, P> processor,
        PartitionedMapper mapper
    ) {
        return mapper.mapSuppliedByteSegs(processor);
    }
}
