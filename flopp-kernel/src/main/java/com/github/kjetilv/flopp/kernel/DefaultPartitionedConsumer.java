package com.github.kjetilv.flopp.kernel;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

class DefaultPartitionedConsumer implements PartitionedConsumer {

    private final PartitionedMapper mapper;

    DefaultPartitionedConsumer(PartitionedMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public Stream<CompletableFuture<PartitionResult<Void>>> forEachLine(
        BiConsumer<Partition, Stream<String>> consumer
    ) {
        return mapper.mapLines((partition, entries) -> {
            consumer.accept(partition, entries);
            return null;
        });
    }

    @Override
    public Stream<CompletableFuture<PartitionResult<Void>>> forEachRawLine(
        BiConsumer<Partition, Stream<byte[]>> consumer
    ) {
        return mapper.mapRawLines((partition, entries) -> {
            consumer.accept(partition, entries);
            return null;
        });
    }

    @Override
    public Stream<CompletableFuture<PartitionResult<Void>>> forEachNLine(
        BiConsumer<Partition, Stream<NLine>> consumer
    ) {
        return mapper.mapNLines((partition, entries) -> {
            consumer.accept(partition, entries);
            return null;
        });
    }

    @Override
    public Stream<CompletableFuture<PartitionResult<Void>>> forEachRNLine(
        BiConsumer<Partition, Stream<RNLine>> consumer
    ) {
        return mapper.mapRNLines((partition, entries) -> {
            consumer.accept(partition, entries);
            return null;
        });
    }

    @Override
    public Stream<CompletableFuture<PartitionResult<ByteSeg>>> forEachByteSeg(
        BiConsumer<Partition, Stream<ByteSeg>> consumer
    ) {
        return mapper.mapByteSegs((partition, entries) -> {
            consumer.accept(partition, entries);
            return null;
        });
    }

    @Override
    public Stream<CompletableFuture<PartitionResult<Supplier<ByteSeg>>>> forEachSuppliedByteSeg(
        BiConsumer<Partition, Stream<Supplier<ByteSeg>>> consumer
    ) {
        return mapper.mapSuppliedByteSegs((partition, entries) -> {
            consumer.accept(partition, entries);
            return null;
        });
    }
}
