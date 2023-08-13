package com.github.kjetilv.lopp;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class FastPartitionedFile implements PartitionedFile {

    private final Path path;

    private final FileShape fileShape;

    private final Partitioning partitioning;

    private final ExecutorService executorService;

    private final List<Partition> partitions;

    public FastPartitionedFile(
        Path path, FileShape fileShape, Partitioning partitioning, ExecutorService executorService
    ) {
        this.path = path;
        this.fileShape = fileShape;
        this.partitioning = partitioning;
        this.executorService = executorService;
        this.partitions =
            Partition.partitions(this.fileShape.fileSize(), null, this.partitioning.partitionCount());
    }

    @Override
    public <T> Stream<CompletableFuture<Result<T>>> asyncMap(
        int sliceSize,
        PartitionBytesProcessor<T> fun,
        ExecutorService executorService
    ) {
        return null;
//        return partitions.stream()
//            .filter(partition -> !partition.empty())
//            .map(partition ->
//                new FastPartitionedFile(path, fileShape, partitioning, executorService))
//            .map(streamer ->
//                stream(streamer, fun, executorService));
    }

    @Override
    public Path process(
        Function<String, Stream<String>> processor,
        Path targetPath,
        ExecutorService executorService,
        int sliceSize
    ) {
        return null;
    }

    @Override
    public Stream<PartitionStreamer> streamers(int sliceSize) {
        RandomAccessFile randomAccessFile = randomAccess(path);
        return partitions.stream()
            .<PartitionStreamer>map(partition ->
                new FastPartitionStreamer(randomAccessFile, partition, fileShape, sliceSize))
            .onClose(() ->
                close(randomAccessFile));
    }

    @Override
    public Path path() {
        return path;
    }

    @Override
    public int partitionCount() {
        return partitioning.partitionCount();
    }

    @Override
    public FileShape fileShape() {
        return fileShape;
    }

    @Override
    public void close() {

    }
    private <T> CompletableFuture<Result<T>> stream(
        FastPartitionStreamer streamer, PartitionProcessor<T> fun, ExecutorService executorService
    ) {
        Executor executor = resolve(executorService);
        return null;
//        CompletableFuture.supplyAsync(() ->
//                fun.handle(streamer.partition(), streamer.lines()), executor)
//            .thenApply(result ->
//                new Result<>(streamer.partition(), result));
    }

    private Executor resolve(ExecutorService executorService) {
        return executorService != null
            ? executorService
            : this.executorService != null ? this.executorService : ForkJoinPool.commonPool();
    }

    private static void close(RandomAccessFile randomAccessFile) {
        try {
            randomAccessFile.close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close: " + randomAccessFile, e);
        }
    }

    private static RandomAccessFile randomAccess(Path path) {
        try {
            return new RandomAccessFile(path.toFile(), "r");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read: " + path, e);
        }
    }
}
