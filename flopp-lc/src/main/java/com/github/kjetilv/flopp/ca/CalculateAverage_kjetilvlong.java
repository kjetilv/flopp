/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.github.kjetilv.flopp.ca;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.bits.Bitwise;
import com.github.kjetilv.flopp.kernel.readers.Column;
import com.github.kjetilv.flopp.kernel.readers.Reader;
import com.github.kjetilv.flopp.kernel.readers.Readers;
import com.github.kjetilv.flopp.kernel.util.Maps;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class CalculateAverage_kjetilvlong {

    public static void main(String[] args) {
        for (String arg : args) {
            Path path = Path.of(arg);
//            go(path);
//            go3(path);
//            go3(path);
//            go3(path);
            go3(path);
//            go3(path);
//            go3(path);
//            go4It(path);
//            go1(path);
        }
    }

    public static Map<String, Result> go(Path path) {
        return go(path, false);
    }

    public static Map<String, Result> go(Path path, boolean slow) {
        return go(path, slow, null);
    }

    public static Map<String, Result> go(Path path, boolean slow, Consumer<SeparatedLine> callbacks) {
        Instant start = Instant.now();
        Shape shape = Shape.of(path, UTF_8).longestLine(128);
        Partitioning partitioning = Partitioning.create(
            Runtime.getRuntime().availableProcessors(),
            shape.longestLine()
        );
        int chunks = partitioning.of(shape.size()).size();
        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path, partitioning, shape);
            ExecutorService executor = slow ? null : new ThreadPoolExecutor(
                chunks,
                chunks,
                0, TimeUnit.NANOSECONDS,
                new LinkedBlockingQueue<>(chunks)
            )
        ) {
            System.out.println(Duration.between(start, Instant.now()));
            PartitionedSplitters partitionedSplitters = bitwisePartitioned.splitters();
            CsvFormat format = new CsvFormat.Escaped(';', '\\');
            List<Supplier<Map<String, Result>>> mapSuppliers =
                partitionedSplitters.splitters(format)
                    .map(splitsConsumer -> {
                        Supplier<Map<String, Result>> worker = () ->
                            toMap(path, splitsConsumer, callbacks);
                        return slow
                            ? worker
                            : joiner(future(worker, executor));
                    })
                    .toList();
            List<Map<String, Result>> maps = mapSuppliers.stream()
                .map(Supplier::get)
                .toList();
            Map<String, Result> combinedMaps = combineMaps(maps);
            System.out.println(combinedMaps);
            System.out.println(Duration.between(start, Instant.now()));
            return combinedMaps;
        }
    }

    private CalculateAverage_kjetilvlong() {
    }

    private static <T> CompletableFuture<T> future(Supplier<T> mapSupplier, ExecutorService executor) {
        return CompletableFuture.supplyAsync(mapSupplier, executor);
    }

    private static <T> Supplier<T> joiner(CompletableFuture<T> future) {
        return future::join;
    }

    @SuppressWarnings("unused")
    private static void go3(Path path) {
        Instant start = Instant.now();
        Shape shape = Shape.of(path, UTF_8).longestLine(128);
        int cpus = Runtime.getRuntime().availableProcessors();
        Partitioning partitioning = Partitioning.create(
            cpus * 5,
            shape.longestLine()
        )
//            .fragment(
//            new TrailFragmentation(
//                cpus * 25,
//                1.0d,
//                0.01d,
//                0.1d
//            )
//        );
        ;
        CsvFormat format = new CsvFormat.Simple(2, ';');
        int chunks = partitioning.of(shape.size()).size();
        AtomicInteger threads = new AtomicInteger();
        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path, partitioning, shape);
            ExecutorService executor =
//                 Executors.newWorkStealingPool()
//                Executors.newVirtualThreadPerTaskExecutor()
//                new ForkJoinPool(
//                    cpus,
//                    ForkJoinPool.defaultForkJoinWorkerThreadFactory,
//                    (t, e) ->
//                        e.printStackTrace(System.err),
//                    true
//                )
                new ThreadPoolExecutor(
                    Runtime.getRuntime().availableProcessors(),
                    Runtime.getRuntime().availableProcessors(),
                    0, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(chunks),
                    r -> new Thread(r, "r" + threads.getAndIncrement())
                )
        ) {
            List<Map<String, Result>> maps = bitwisePartitioned.splitters(format)
                .map(splitter ->
                    CompletableFuture.supplyAsync(() -> map(splitter), executor))
                .toList()
                .stream()
                .map(CompletableFuture::join)
                .toList();
            Map<String, Result> map = combineMaps(maps);
            System.out.println(map);
            System.out.println(Duration.between(start, Instant.now()));
            System.out.println(map.keySet()
                                   .stream().mapToInt(String::length).sum() / map.size());
        }
    }

    private static Map<String, Result> combineMaps(List<Map<String, Result>> maps) {
        Set<String> keys = maps.stream()
            .map(Map::keySet).flatMap(Collection::stream)
            .collect(Collectors.toSet());
        Map<String, Result> treeMap = new TreeMap<>(maps.getFirst());
        maps.stream().skip(1)
            .forEach(map ->
                keys.forEach(key ->
                    treeMap.put(
                        key,
                        treeMap.computeIfAbsent(key, _ -> new Result(0)).merge(map.get(key))
                    )));
        return treeMap;
    }

    private static Map<String, Result> map(PartitionedSplitter splitter) {
        Map<String, Result> m = Maps.ofSize(512);
        Readers.create(
            Column.ofString("station", 0, new byte[128], UTF_8),
            Column.ofInt("measurement", 1, CalculateAverage_kjetilvlong::parseValue)
        ).read(splitter, columns ->
            m.compute(
                (String) columns.get("station"),
                (_, existing) -> {
                    int dec = columns.getInt("measurement");
                    return existing == null
                        ? new Result(dec)
                        : existing.collect(dec);
                }
            ));
        return m;
    }

    @SuppressWarnings("unused")
    private static void go4It(Path path) {
        Instant start = Instant.now();
        Shape shape = Shape.of(path, UTF_8).longestLine(128);
        Partitioning partitioning = Partitioning.create(
            Runtime.getRuntime().availableProcessors(),
            shape.longestLine()
        ).scaled(2);
        CsvFormat format = new CsvFormat.Simple(2, ';');
        Reader reader = Readers.create(
            Column.ofString("station", 1),
            Column.ofInt("measurement", 2, CalculateAverage_kjetilvlong::parseValue)
        );
        int chunks = partitioning.of(shape.size()).size();
        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path, partitioning, shape);
            ExecutorService executor = new ThreadPoolExecutor(
                chunks,
                chunks,
                0, TimeUnit.NANOSECONDS,
                new LinkedBlockingQueue<>(chunks)
            )
        ) {
            System.out.println(Duration.between(start, Instant.now()));
            PartitionedSplitters partitionedSplitters = bitwisePartitioned.splitters();
            Stream<PartitionedSplitter> partitionStreamers = partitionedSplitters.splitters(format);
            List<CompletableFuture<Map<LineSegment, Result>>> list = partitionStreamers.map(splitter ->
                    CompletableFuture.supplyAsync(
                        () -> {
                            Map<LineSegment, Result> results = Maps.ofSize(512);
                            reader.read(splitter, columns -> {
                                LineSegment station = (LineSegment) columns.get("station");
                                int dec = (Integer) columns.get("measurement");
                                results.compute(
                                    station.copy(),
                                    (key, existing) ->
                                        existing == null
                                            ? new Result(dec)
                                            : existing.collect(dec)
                                );
                            });
                            return results;
                        },
                        executor
                    ))
                .toList();
            List<Map<LineSegment, Result>> maps = list.stream()
                .map(CompletableFuture::join)
                .toList();
            System.out.println(Duration.between(start, Instant.now()));
            Set<LineSegment> keys = keySet(maps);
            Map<String, Result> map = combineMapsToStringKey(keys, maps);
            System.out.println(map);
            System.out.println(Duration.between(start, Instant.now()));
        }
    }

    private static Map<String, Result> toMap(
        Path path,
        PartitionedSplitter splitsConsumer,
        Consumer<SeparatedLine> callbacks
    ) {
        Map<String, Result> m = new HashMap<>(1024, 1.0f);
        try {
            splitsConsumer.forEach(csvLine -> {
                if (callbacks != null) {
                    callbacks.accept(csvLine);
                }
                parse(csvLine, m);
            });
        } catch (Exception e) {
            throw new IllegalStateException("Failed with " + path, e);
        }
        return m;
    }

    private static void parse(SeparatedLine separatedLine, Map<String, Result> m) {
        try {
            String segment = separatedLine.segment(0).asString(UTF_8);
            int measure = parseValue(separatedLine.segment(1));
            m.compute(
                segment,
                (_, existing) ->
                    existing == null
                        ? new Result(measure)
                        : existing.collect(measure)
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> Set<T> keySet(List<Map<T, Result>> maps) {
        return maps.stream()
            .map(Map::keySet).flatMap(Collection::stream)
            .collect(Collectors.toSet());
    }

    private static SequencedMap<String, Result> combineMapsToStringKey(
        Set<LineSegment> keys,
        List<Map<LineSegment, Result>> submaps
    ) {
        SequencedMap<LineSegment, Result> map = new TreeMap<>();
        submaps.forEach(submap ->
            keys.forEach(key -> {
                Result base = map.get(key);
                Result addendum = submap.get(key);
                if (base == null) {
                    map.put(key, addendum);
                } else if (addendum != null) {
                    Result merged = base.merge(addendum);
                    map.put(key, merged);
                }
            }));
        SequencedMap<String, Result> seqMap = new TreeMap<>();
        map.forEach((segment, result) -> seqMap.put(segment.asString(UTF_8), result));
        return seqMap;
    }

    private static int semiIndex(LineSegment ls, long length) {
        for (int i = Math.toIntExact(length - 3); i >= 0; i--) {
            if (ls.byteAt(i) == ';') {
                return i;
            }
        }
        throw new IllegalStateException("No split in " + ls.asString(UTF_8));
    }

    private static int parseValue(int length, int splitIndex, LineSegment ls) {
        int value = 0;
        int boundary = splitIndex + 1;
        int pos = 1;
        for (int i = length - 1; i >= boundary; i--) {
            byte b = ls.byteAt(i);
            if (b == '.') {
                continue;
            }
            if (b == '-') {
                return value * -1;
            }
            value += (b - '0') * pos;
            pos *= 10;
        }
        return value;
    }

    private static int parseValue(LineSegment segment) {
        int value = 0;
        int pos = 1;
        long head = segment.unalignedLongNo(0);
        long len = segment.length();
        for (long i = len - 1; i >= 0; i--) {
            long shift = i * 8;
            long b = head >> shift & 0xFF;
            if (b == '.') {
                continue;
            }
            if (b == '-') {
                return value * -1;
            }
            int j = (int) (b - '0');
            value += j * pos;
            pos *= 10;
        }
        return value;
    }

    public static final class Result {

        private int count;

        private int min;

        private int max;

        private int sum;

        private Result(int value) {
            this.min = value;
            this.max = value;
            this.sum = value;
            this.count = 1;
        }

        public String toString() {
            return round(min) + "/" + round(1.0 * sum / count) + "/" + round(max);
        }

        public Result merge(Result coll) {
            if (coll == null) {
                return this;
            }
            min = Math.min(min, coll.min);
            max = Math.max(max, coll.max);
            sum += coll.sum;
            count += coll.count;
            return this;
        }

        Result collect(int value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
            return this;
        }

        private static double round(double value) {
            return Math.round(value) / 10.0;
        }
    }
}
