package com.github.kjetilv.lopp;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface PartitionedFile extends Closeable {

    static PartitionedFile create(
        Path path,
        FileShape fileShape,
        Partitioning partitioning
    ) {
        return new DefaultPartitionedFile(
            Objects.requireNonNull(path, "path"),
            Objects.requireNonNull(fileShape, "fileShape"),
            partitioning,
            null);
    }

    static PartitionedFile create(
        Path path,
        FileShape fileShape,
        Partitioning partitioning,
        ExecutorService executorService
    ) {
        return new DefaultPartitionedFile(
            Objects.requireNonNull(path, "path"),
            Objects.requireNonNull(fileShape, "fileShape"),
            partitioning,
            Objects.requireNonNull(executorService, "executorService"));
    }

    private static <T> List<T> await(Stream<CompletableFuture<T>> futures) {
        return await(futures.toList());
    }

    private static <T> List<T> await(List<CompletableFuture<T>> futures) {
        return futures.stream()
            .map(CompletableFuture::join)
            .toList();
    }

    default List<Result<Void>> awaitForEach(
        int sliceSize,
        PartitionConsumer fun
    ) {
        return awaitForEach(sliceSize, fun, null);
    }

    default List<Result<Void>> awaitForEach(
        int sliceSize,
        PartitionConsumer fun,
        ExecutorService executorService
    ) {
        return await(asyncForEach(sliceSize, fun, executorService));
    }

    default Stream<CompletableFuture<Result<Void>>> asyncForEach(
        int sliceSize,
        PartitionConsumer fun,
        ExecutorService executorService
    ) {
        return asyncMap(
            sliceSize,
            (partitionBytes, entries) -> {
                fun.handle(partitionBytes, entries);
                return null;
            },
            executorService
        );
    }

    default <T> List<Result<T>> awaitMap(
        int sliceSize,
        PartitionBytesProcessor<T> fun
    ) {
        return awaitMap(sliceSize, fun, null);
    }

    default <T> List<Result<T>> awaitMap(
        int sliceSize,
        PartitionBytesProcessor<T> fun,
        ExecutorService executorService
    ) {
        return await(asyncMap(sliceSize, fun, executorService));
    }

    <T> Stream<CompletableFuture<Result<T>>> asyncMap(
        int sliceSize,
        PartitionBytesProcessor<T> fun,
        ExecutorService executorService
    );

    default Path processSingle(
        Function<String, String> processor,
        Path targetPath,
        ExecutorService executorService
    ) {
        return processSingle(
            processor,
            targetPath,
            executorService,
            0
        );
    }

    default Path processSingle(
        Function<String, String> processor,
        Path targetPath,
        ExecutorService executorService,
        int slizeSize
    ) {
        return process(
            line ->
                Stream.of(processor.apply(line)),
            targetPath,
            executorService,
            slizeSize
        );
    }

    Path process(
        Function<String, Stream<String>> processor,
        Path targetPath,
        ExecutorService executorService,
        int sliceSize
    );

    default Stream<PartitionStreamer> streamers() {
        return streamers(0);
    }

    Stream<PartitionStreamer> streamers(int sliceSize);

    Path path();

    int partitionCount();

    FileShape fileShape();

    @Override
    void close();

    record Result<T>(PartitionBytes partitionBytes, T result) implements Comparable<Result<T>> {

        public Result(PartitionBytes partitionBytes, T result) {
            this.partitionBytes = Objects.requireNonNull(partitionBytes, "partitionBytes");
            this.result = Objects.requireNonNull(result, "result");
        }

        @Override
        public int compareTo(Result o) {
            return partitionBytes.compareTo(o.partitionBytes());
        }

        public Partition partition() {
            return partitionBytes().partition();
        }

        public Partition bytesPartition() {
            return partitionBytes().toPartition();
        }

        public Result<T> asPartition(long offset, long count) {
            return new Result<>(partitionBytes.at(offset, count), result);
        }
    }

}
