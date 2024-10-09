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
public abstract sealed class AbstractPartitionedProcessor<I, O>
    implements PartitionedProcessor<I, O>
    permits FormatPartitionedProcessor, LinePartitionedProcessor {

    private final Partitioned<Path> partitioned;

    private final Path target;

    public AbstractPartitionedProcessor(Partitioned<Path> partitioned, Path target) {
        this.partitioned = Objects.requireNonNull(partitioned, "partitioned");
        this.target = Objects.requireNonNull(target, "target");
    }

    protected final Partitioned<Path> partitioned() {
        return partitioned;
    }

    protected final Path target() {
        return target;
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
