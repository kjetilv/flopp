package com.github.kjetilv.lopp;

import com.github.kjetilv.lopp.lc.IndexingLineCounter;
import com.github.kjetilv.lopp.lc.LineCounter;
import com.github.kjetilv.lopp.lw.LinesWriter;
import com.github.kjetilv.lopp.lw.LinesWriters;
import com.github.kjetilv.lopp.lw.SimpleLinesWriter;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Stream;

final class DefaultPartitionedFile implements PartitionedFile {

    private final RandomAccessFile file;

    private final FileShape fileShape;

    private final Path path;

    private final Partitioning partitioning;

    private final ExecutorService executorService;

    private final List<PartitionBytes> partitionBytesList;

    DefaultPartitionedFile(Path path, FileShape fileShape, Partitioning partitioning, ExecutorService service) {
        this.path = Objects.requireNonNull(path, "path");
        if (!Files.isRegularFile(this.path)) {
            throw new IllegalStateException("Invalid file: " + path);
        }
        FileShape shape = fileShape.hasStats() ? fileShape : fileShape.fileSize(fileSize());
        this.partitioning = Objects.requireNonNull(partitioning, "partitioning");
        this.executorService = service;
        LineCounter.Lines lines = scan(path, shape);
        this.partitionBytesList = lines.bytesPartitions();
        this.fileShape = shape.stats(lines.linesCount(), lines.longestLine());
        this.file = randomAccess(path);
    }

    @Override
    public <T> Stream<CompletableFuture<Result<T>>> asyncMap(
        int sliceSize,
        PartitionBytesProcessor<T> fun,
        ExecutorService executorService
    ) {
        return partitionBytesList.stream()
            .filter(partition -> !partition.empty())
            .map(partition ->
                new DefaultPartitionStreamer(file, partition, sliceSize, fileShape))
            .map(streamer ->
                stream(streamer, fun, executorService));
    }

    @Override
    public Path process(
        Function<String, Stream<String>> processor,
        Path targetPath,
        ExecutorService executor,
        int sliceSize
    ) {
        int bufferSize = sliceSize > 0 ? sliceSize : SLICE_SIZE;
        PathResultConsumer pathResultConsumer = new PathResultConsumer(partitionCount());
        Path tempDirectory = workDir();
        executor.execute(() ->
            asyncMap(
                bufferSize,
                (partitionBytes, npLines) -> {
                    Path sub = tmpFile(tempDirectory, partitionBytes);
                    try (LinesWriter linesWriter = LinesWriters.simple(sub, fileShape.charset())) {
                        npLines.map(NPLine::line).flatMap(processor)
                            .forEach(linesWriter);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to write " + sub, e);
                    }
                    return sub;
                },
                executor
            ).forEach(pathResultConsumer));
        transfer(targetPath, pathResultConsumer.stream());
        return targetPath;
    }

    @Override
    public Stream<PartitionStreamer> streamers(int sliceSize) {
        return partitionBytesList.stream()
            .filter(partition -> !partition.empty())
            .map(partition ->
                new DefaultPartitionStreamer(file, partition, sliceSize, fileShape));
    }

    @Override
    public Path path() {
        return path;
    }

    @Override
    public int partitionCount() {
        return partitionBytesList.size();
    }

    @Override
    public FileShape fileShape() {
        return fileShape;
    }

    @Override
    public void close() {
        try {
            file.close();
        } catch (IOException e) {
            throw new IllegalStateException(this + " failed to close", e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + path + ": " + fileShape + " partitions:" + partitionBytesList.size() + "]";
    }

    private long fileSize() {
        long size;
        try {
            size = Files.size(path);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to assert size of " + path, e);
        }
        if (size > 0) {
            return size;
        }
        throw new IllegalStateException("Empty file: " + path);
    }

    private LineCounter.Lines scan(Path path, FileShape sizedShape) {
        return new IndexingLineCounter(this.partitioning, sizedShape).scan(path);
    }

    private <T> CompletableFuture<Result<T>> stream(
        DefaultPartitionStreamer streamer, PartitionBytesProcessor<T> fun, ExecutorService executorService
    ) {
        Executor executor = resolve(executorService);
        return CompletableFuture.supplyAsync(() ->
                fun.handle(streamer.partitionBytes(), streamer.lines()), executor)
            .thenApply(result ->
                new Result<>(streamer.partitionBytes(), result));
    }

    private Path tmpFile(Path tempDirectory, PartitionBytes partitionBytes) {
        String string = path.getFileName().toString();
        int suffixIndex = string.lastIndexOf('.');
        String suffix = suffixIndex < 0 ? "" : string.substring(suffixIndex + 1);
        int no = partitionBytes.partition().partitionNo();
        String tmpFilename = string + "-" + no + ".tmp" + (suffixIndex < 0 ? "" : "." + suffix);
        return tempDirectory.resolve(tmpFilename);
    }

    private Path workDir() {
        Path tempDirectory;
        try {
            String dir = "workdir-" + path.getFileName() + "-tmp";
            tempDirectory = Files.createTempDirectory(dir);
        } catch (IOException e) {
            throw new IllegalStateException(this + ": Failed to create temp dir", e);
        }
        return tempDirectory;
    }

    private Executor resolve(ExecutorService executorService) {
        return executorService != null
            ? executorService
            : this.executorService != null ? this.executorService : ForkJoinPool.commonPool();
    }

    private static final int SLICE_SIZE = 64 * 1024;

    private static RandomAccessFile randomAccess(Path path) {
        try {
            return new RandomAccessFile(path.toFile(), "r");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read: " + path, e);
        }
    }

    private static long fileSize(Path path) {
        try {
            return Files.size(path);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to assert size of " + path, e);
        }
    }

    private static void transfer(Path tmp, Stream<PartitionedFile.Result<Path>> results) {
        try (
            RandomAccessFile target = new RandomAccessFile(tmp.toFile(), "rw");
            FileChannel channel = target.getChannel()
        ) {
            results.map(result -> new PartitionTransferer(result.result(), result.bytesPartition()))
                .<Runnable>map(transfer -> () -> transfer.accept(channel))
                .map(CompletableFuture::runAsync)
                .toList()
                .forEach(CompletableFuture::join);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
