package com.github.kjetilv.flopp.lc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.github.kjetilv.flopp.Shape;

public final class Lc {

    public static void main(String[] args) throws Exception {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        try (
            ExecutorService executor = new ThreadPoolExecutor(
                availableProcessors * 10,
                availableProcessors * 20,
                10, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2000)
            )
        ) {
            AsyncLineCounter counter = new AsyncLineCounter(executor, 1024 * 1024);
            Stream<Path> paths = paths(args);
//            countAsync(paths, counter, executor);
            System.out.println("  " + countSync(paths, counter));
        }
    }

    private Lc() {

    }

    private static final LongAdder SUM = new LongAdder();

    private static final LongAdder COUNTED = new LongAdder();

    private static final List<CompletableFuture<Count>> COUNTS = new ArrayList<>();

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
                COUNTED.increment();
                System.out.println(count);
            })
            .mapToLong(Count::lines)
            .sum();
    }

    private static void countAsync(Stream<Path> paths, AsyncLineCounter counter, ExecutorService executor) {
        paths.forEach(path ->
            COUNTS.add(CompletableFuture.supplyAsync(() -> count(counter, path), executor)
                .exceptionally(throwable ->
                    uncountable(path, throwable))
                .thenApplyAsync(count -> {
                    SUM.add(count.lines());
                    COUNTED.increment();
                    return count;
                })));
        COUNTS.stream()
            .map(CompletableFuture::join)
            .forEach(System.out::println);
        if (COUNTS.size() > 1) {
            System.out.println("  " + SUM);
        }
    }

    @SuppressWarnings("resource")
    private static Stream<Path> paths(String... args) {
        return Arrays.stream(args).flatMap(arg -> {
            Path root = Paths.get(".");
            if (arg.startsWith("**/*.")) {
                String suffix = arg.substring("**/*".length());
                return walk(root, suffixed(suffix));
            }
            if (arg.startsWith("*.")) {
                String suffix = arg.substring("*".length());
                try {
                    Stream<Path> list = Files.list(root);
                    return list.filter(suffixed(suffix));
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

    @SuppressWarnings("resource")
    private static Stream<Path> walk(Path root, Predicate<Path> predicate) {
        try {
            Stream<Path> list = Files.list(root);
            return list.flatMap(path -> {
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
        SUM.add(lines);
        return new Count(path, lines);
    }

    private static Count uncountable(Path path, Throwable throwable) {
        throwable.printStackTrace(System.err);
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
