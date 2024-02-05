package com.github.kjetilv.flopp.lc;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.bits.Bitwise;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

public final class Lccc {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage:");
            System.out.println("  lc <file>");
            System.exit(0);
        }
        Instant now = Instant.now();
        long total = count(Arrays.stream(args)
            .map(Path::of));
        System.out.println("    " + total);
        System.out.println(Duration.between(now, Instant.now()));
    }

    private Lccc() {

    }

    @SuppressWarnings("unused")
    private static long count(Stream<Path> paths) {
        return paths.map(path -> {
                try {
                    return count(path);
                } catch (Exception e) {
                    return uncountable(path, e);
                }
            }).peek(System.out::println)
            .mapToLong(Count::lines)
            .sum();
    }

    private static Count count(Path path) {
        Shape shape = Shape.of(path).longestLine(100);
        int cpus = Runtime.getRuntime().availableProcessors();
        Partitioning partitioning = Partitioning
            .create(cpus, 128)
            .scaled(2);
        Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path, partitioning, shape);
        Stream<? extends PartitionStreamer> streamers = bitwisePartitioned.streams().streamers();
        List<CompletableFuture<Long>> futures = streamers
            .map(streamer ->
                CompletableFuture.supplyAsync(
                    () -> count(streamer),
                    new ForkJoinPool(cpus * 2)
                ))
            .onClose(streamers::close)
            .toList();
        long sum = futures.stream().mapToLong(CompletableFuture::join).sum();
        return new Count(path, sum);
    }

    @SuppressWarnings("unused")
    private static Count countSync(Path path) {
        List<Long> futures;
        Shape shape = Shape.of(path).longestLine(100);
        Partitioning partitioning = Partitioning.create(Runtime.getRuntime().availableProcessors(), 64);
        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path, partitioning, shape);
            PartitionedStreams streamers = bitwisePartitioned.streams()
        ) {
            futures = streamers.streamers()
                .map(Lccc::count)
                .toList();
        }
        long sum = futures.stream().mapToLong(Long::longValue).sum();
        return new Count(path, sum);
    }

    private static long count(PartitionStreamer streamer) {
        LongAdder longAdder = new LongAdder();
        streamer.lines()
            .forEach(_ -> longAdder.increment());
        return longAdder.longValue();
    }

    private static Count uncountable(Path path, Throwable throwable) {
        if (throwable == null) {
            return new Count(path, -1L);
        }
        throwable.printStackTrace(System.err);
        return new Count(path, -1L);
    }

    record Count(Path path, Long lines) {

        @Override
        public String toString() {
            return String.format("%1$10s %2$s", lines, path);
        }
    }
}
