package com.github.kjetilv.flopp.lc;

import com.github.kjetilv.flopp.Shape;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.stream.Stream;

public final class Lc {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage:");
            System.out.println("  lc <file>");
            System.exit(0);
        }
        ExecutorService service = Executors.newVirtualThreadPerTaskExecutor();
        AsyncLineCounter counter = new AsyncLineCounter(service, 1024 * 1024);
        if (args.length > 1 || Arrays.stream(args).anyMatch(arg -> arg.contains("*"))) {
            Stream<Path> paths = paths(args);
            countAsync(paths, counter, service);
        } else {
            System.out.println(count(counter, Paths.get(args[0])));
        }
    }

    private Lc() {

    }

    private static final LongAdder SUM = new LongAdder();

    @SuppressWarnings("unused")
    private static long countSync(Stream<Path> paths, AsyncLineCounter counter) {
        return paths.map(path -> {
                try {
                    return count(counter, path);
                } catch (Exception e) {
                    return uncountable(path, e);
                }
            }).peek(count -> {
                SUM.add(count.lines());
                System.out.println(count);
            })
            .mapToLong(Count::lines)
            .sum();
    }

    private static void countAsync(Stream<Path> paths, AsyncLineCounter counter, ExecutorService executor) {
        ArrayBlockingQueue<CompletableFuture<Count>> queue = new ArrayBlockingQueue<>(1);
        Stream<CompletableFuture<Count>> countFutures = paths.map(path ->
            CompletableFuture.supplyAsync(
                () -> count(counter, path),
                executor
            ).exceptionally(throwable ->
                uncountable(path, throwable)));
        CompletableFuture<Count> poison = CompletableFuture
            .supplyAsync(() -> new Count(null, 0L), executor);

        CompletableFuture<Void> feeder = CompletableFuture.runAsync(() ->
            Stream.concat(countFutures, Stream.of(poison))
                .forEach(future -> {
                    try {
                        queue.put(future);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                })).whenComplete((__, e) -> fail(e));

        LongAdder sum = new LongAdder();
        CompletableFuture<Void> eater = CompletableFuture.runAsync(
            () -> {
                while (true) {
                    CompletableFuture<Count> take;
                    try {
                        take = queue.take();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    if (take == poison) {
                        return;
                    }
                    Count join = take.join();
                    System.out.println(join);
                    sum.add(join.lines());
                }
            },
            executor
        ).whenComplete((__, e) -> fail(e));

        eater.join();
        feeder.join();
        System.out.println("    " + sum);
    }

    private static void fail(Throwable e) {
        if (e != null) {
            e.printStackTrace(System.err);
        }
    }

    private static Stream<Path> paths(String... args) {
        return Arrays.stream(args).flatMap(arg -> {
            Path root = Paths.get(".");
            if (arg.startsWith("**/*.")) {
                return walk(root, suffixed(arg, 4));
            }
            if (arg.startsWith("*.")) {
                try (Stream<Path> list = Files.list(root)) {
                    return list.filter(suffixed(arg, 1));
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to list " + root, e);
                }
            } else {
                Path path = Paths.get(arg);
                if (Files.isRegularFile(path)) {
                    return Stream.of(path);
                } else {
                    return Stream.empty();
                }
            }
        });
    }

    private static Predicate<Path> suffixed(String arg, int length) {
        return suffixed(arg.substring(length));
    }

    @SuppressWarnings("resource")
    private static Stream<Path> walk(Path root, Predicate<Path> predicate) {
        try {
            return Files.list(root).flatMap(path -> {
                if (Files.isRegularFile(path) && predicate.test(path)) {
                    return Stream.of(path);
                }
                if (Files.isDirectory(path)) {
                    return walk(path, predicate);
                }
                return Stream.empty();
            });
        } catch (Exception e) {
            throw new IllegalStateException("Failed to walk from " + root, e);
        }
    }

    private static Predicate<Path> suffixed(String suffix) {
        return path ->
            path.getFileName().toString().endsWith(suffix);
    }

    private static Count count(AsyncLineCounter counter, Path path) {
        Shape shape = shapeOf(path);
        long lines = counter.count(path, shape);
        return new Count(path, lines);
    }

    private static Count uncountable(Path path, Throwable throwable) {
        fail(throwable);
        return new Count(path, -1L);
    }

    private static Shape shapeOf(Path path) {
        return Shape.size(getSize(path));
    }

    private static long getSize(Path path) {
        long size;
        try {
            size = Files.size(path);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to size up " + path, e);
        }
        return size;
    }

    record Count(Path path, Long lines) {

        @Override
        public String toString() {
            return String.format("%1$10s %2$s", lines, path);
        }
    }
}
