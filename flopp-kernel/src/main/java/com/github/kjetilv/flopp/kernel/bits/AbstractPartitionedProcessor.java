package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.PartitionResult;
import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.PartitionedProcessor;
import com.github.kjetilv.flopp.kernel.Shape;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.ToLongFunction;

@SuppressWarnings("preview")
abstract sealed class AbstractPartitionedProcessor<T, I, O>
    implements PartitionedProcessor<T, I, O>
    permits FormatPartitionedProcessor, LinePartitionedProcessor {

    private final Partitioned<T> partitioned;

    protected AbstractPartitionedProcessor(Partitioned<T> partitioned) {
        this.partitioned = Objects.requireNonNull(partitioned, "partitioned");
    }

    protected final Partitioned<T> partitioned() {
        return partitioned;
    }

    protected static ToLongFunction<Path> sizer() {
        return path -> Shape.of(path).size();
    }

    protected static void join(StructuredTaskScope<PartitionResult<Path>> scope) {
        try {
            scope.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted", e);
        }
    }
}
