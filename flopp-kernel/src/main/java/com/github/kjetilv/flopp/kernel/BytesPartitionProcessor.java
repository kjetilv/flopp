package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

class BytesPartitionProcessor<P> extends AbstractPartitionProcessor<P, byte[]> {

    BytesPartitionProcessor(
        PartitionedMapper partitionedMapper,
        Charset charset,
        int partitionCount,
        LinesWriterFactory<P> linesWriterFactory,
        TempTargets<P> tempTargets,
        ToLongFunction<P> sizer,
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
        BiFunction<Partition, Stream<byte[]>, P> processor,
        PartitionedMapper mapper
    ) {
        return mapper.mapRawLines(processor);
    }

}
