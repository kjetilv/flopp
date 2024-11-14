package com.github.kjetilv.flopp.lc;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.files.PartitionedPaths;
import com.github.kjetilv.flopp.kernel.partitions.Partitioning;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class Lc {

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

    private Lc() {

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
        Shape shape = Shape.of(path, UTF_8).longestLine(100);
        int cpus = Runtime.getRuntime().availableProcessors();
        Partitioning partitioning = Partitioning
            .create(cpus, 128)
            .scaled(2);

        try (
            Partitioned<Path> bitwisePartitioned = PartitionedPaths.partitioned(path, partitioning, shape)
        ) {
            List<CompletableFuture<Long>> longSuppliers = bitwisePartitioned.lineCounters()
                .map(counter ->
                    CompletableFuture.supplyAsync(
                        counter::getAsLong,
                        new ForkJoinPool(cpus * 2)
                    ))
                .toList();
            long count = longSuppliers.stream().mapToLong(CompletableFuture::join).sum();
            return new Count(path, count);
        }
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
