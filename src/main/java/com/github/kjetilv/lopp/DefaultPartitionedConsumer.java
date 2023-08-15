package com.github.kjetilv.lopp;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

class DefaultPartitionedConsumer implements PartitionedConsumer {

    private final PartitionedMapper mapper;

    private final ByteSources sources;

    DefaultPartitionedConsumer(
        PartitionedMapper mapper,
        ByteSources sources
    ) {
        this.mapper = mapper;
        this.sources = Objects.requireNonNull(sources, "sources");
    }

    @Override
    public Stream<CompletableFuture<PartitionResult<Void>>> forEach(BiConsumer<Partition, Stream<NpLine>> consumer) {
        return mapper.map(
            (partition, entries) -> {
                consumer.accept(partition, entries);
                return null;
            }
        );
    }

    @Override
    public void close() {
        sources.close();
    }
}
