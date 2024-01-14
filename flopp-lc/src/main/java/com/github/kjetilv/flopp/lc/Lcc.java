package com.github.kjetilv.flopp.lc;

import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.files.PartitionedPaths;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

public final class Lcc {

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

    private Lcc() {

    }

    public static final Partitioning PARTITIONING = Partitioning.defaults(128 * 1024);

    public static final ExecutorService EXECUTOR_SERVICE = Executors.newVirtualThreadPerTaskExecutor();

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
        LongAdder count = new LongAdder();
        PartitionedPaths.create(path, PARTITIONING, EXECUTOR_SERVICE)
            .mapSuppliedByteSegPartition((__, stream) -> stream.count())
            .forEach(count::add);
        return new Count(path, count.longValue());
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
