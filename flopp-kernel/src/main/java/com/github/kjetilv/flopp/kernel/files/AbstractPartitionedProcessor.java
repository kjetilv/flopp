package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.PartitionedProcessor;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.StructuredTaskScope;

@SuppressWarnings("preview")
abstract sealed class AbstractPartitionedProcessor<I, O>
    implements PartitionedProcessor<I, O>
    permits FormatPartitionedProcessor, LinePartitionedProcessor {

    private final Partitioned partitioned;

    protected AbstractPartitionedProcessor(Partitioned partitioned) {
        this.partitioned = Objects.requireNonNull(partitioned, "partitioned");
    }

    protected final Partitioned partitioned() {
        return partitioned;
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
