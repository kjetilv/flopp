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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class CalculateAverage_kjetilvlong {

    public static void main(String[] args) {
        for (String arg : args) {
            Path path = Path.of(arg);
//            go3(path);
//            go3(path);
            go5(path);
//            go4It(path);
//            go3(path);
//            go3(path);
        }
    }

    @SuppressWarnings("unused")
    static Map<String, Result> go3(Path path) {
        return go3(path, null);
    }

    @SuppressWarnings("unused")
    static Map<String, Result> go3(Path path, Partitioning partitioning) {
        Instant start = Instant.now();
        Shape shape = Shape.of(path, UTF_8).longestLine(128);
        int cpus = Runtime.getRuntime().availableProcessors();
        CsvFormat format = new CsvFormat.Simple(2, ';');
        Partitioning p = partitioning == null ? partitioning(cpus, shape) : partitioning;
        int chunks = p.of(shape.size()).size();
        AtomicInteger threads = new AtomicInteger();
        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path, p, shape);
            ExecutorService executor = new ThreadPoolExecutor(
                cpus,
                cpus,
                30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(bitwisePartitioned.partitions().size()),
                r -> new Thread(r, "t" + threads.getAndIncrement()),
                new ThreadPoolExecutor.AbortPolicy()
            )
        ) {
            List<CompletableFuture<Map<String, Result>>> futures =
                bitwisePartitioned.splitters(format, executor)
                    .map(splitterFuture ->
                        splitterFuture.thenApply(CalculateAverage_kjetilvlong::map))
                    .toList();
            executor.shutdown();
            List<Map<String, Result>> maps = futures
                .stream()
                .map(CompletableFuture::join)
                .toList();
            Map<String, Result> map = combineMaps(maps);
            System.out.println(map);
            System.out.println(Duration.between(start, Instant.now()));
            System.out.println(map.keySet()
                                   .stream().mapToInt(String::length).sum() / map.size());
            return map;
        }
    }

    @SuppressWarnings("unused")
    static Map<String, Result> go5(Path path) {
        return go5(path, null);
    }

    @SuppressWarnings("unused")
    static Map<String, Result> go5(Path path, Partitioning partitioning) {
        Instant start = Instant.now();
        Shape shape = Shape.of(path, UTF_8).longestLine(128);
        int cpus = Runtime.getRuntime().availableProcessors();
        CsvFormat format = new CsvFormat.Simple(2, ';');
        Partitioning p = partitioning == null ? partitioning(cpus, shape) : partitioning;
        int chunks = p.of(shape.size()).size();
        AtomicInteger threads = new AtomicInteger();
        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path, p, shape);
            ExecutorService executor =
                new ThreadPoolExecutor(
                    cpus,
                    cpus,
                    30, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(bitwisePartitioned.partitions().size() + 10),
                    r -> new Thread(r, "t" + threads.getAndIncrement()),
                    new ThreadPoolExecutor.AbortPolicy()
                )
        ) {
            List<CompletableFuture<Map<LineSegmentKey, Result>>> futures =
                bitwisePartitioned.splitters(format, executor)
                    .map(splitterFuture ->
                        splitterFuture.thenApply(splitter ->
                            mapLineSegments(
                                splitter,
                                LineSegmentKey::create
//                                new LineSegmentKeyPool()::resolve
                            )))
                    .toList();
            List<Map<String, Result>> maps = futures.stream()
                .map(CompletableFuture::join)
                .map(resultMap ->
                    Maps.mapKeys(resultMap, LineSegmentKey::get)
                )
                .toList();
            Map<String, Result> map = combineMaps(maps);
            System.out.println(map);
            System.out.println(Duration.between(start, Instant.now()));
            System.out.println(map.keySet()
                                   .stream()
                                   .mapToInt(String::length).sum() / map.size());
            return map;
        }
    }

    private CalculateAverage_kjetilvlong() {
    }

    private static Partitioning partitioning(int cpus, Shape shape) {
        if (shape.size() < 1_000_000) {
            return Partitioning.create(cpus, shape.longestLine());
        }
        TrailFragmentation trailFragmentation = new TrailFragmentation(
            cpus * 100,
            2.0d,
            .0001d,
            .05d
        );
        if (shape.size() < 100_000_000) {
            return Partitioning.create(
                cpus * 10,
                shape.longestLine()
            ).fragment(trailFragmentation);
        }
        return Partitioning.create(
            cpus * 100,
            shape.longestLine()
        ).fragment(trailFragmentation);
    }

    private static <T> Map<T, Result> combineMaps(List<Map<T, Result>> maps) {
        Set<T> keys = maps.stream()
            .map(Map::keySet).flatMap(Collection::stream)
            .collect(Collectors.toSet());
        Map<T, Result> treeMap = new TreeMap<>(maps.getFirst());
        maps.stream().skip(1)
            .forEach(map ->
                keys.forEach(key ->
                    treeMap.compute(key, (_, existing) -> existing == null
                        ? map.get(key)
                        : existing.merge(map.get(key)))));
        return treeMap;
    }

    private static Map<String, Result> map(PartitionedSplitter splitter) {
        Map<String, Result> m = new HashMap<>(Maps.mapCapacity(2048));
        Readers.create(
            Column.ofString(0, new byte[128], UTF_8),
            Column.ofInt(1, CalculateAverage_kjetilvlong::parseValue)
        ).read(splitter, columns ->
            m.compute(
                (String) columns.get(0),
                (_, existing) -> {
                    int dec = columns.getInt(1);
                    return existing == null
                        ? new Result(dec)
                        : existing.collect(dec);
                }
            ));
        return m;
    }

    private static Map<LineSegmentKey, Result> mapLineSegments(
        PartitionedSplitter splitter,
        Function<LineSegment, LineSegmentKey> keyFunction
    ) {
        Map<LineSegmentKey, Result> m = new HashMap<>(Maps.mapCapacity(2048));
        Readers.create(
            Column.ofKeySegment(0, keyFunction),
            Column.ofInt(1, CalculateAverage_kjetilvlong::parseValue)
        ).read(splitter, columns ->
            m.compute(
                (LineSegmentKey) columns.get(0),
                (_, existing) -> {
                    int dec = columns.getInt(1);
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
                            Reader reader = Readers.create(
                                Column.ofSegment(0),
                                Column.ofInt(1, CalculateAverage_kjetilvlong::parseValue)
                            );
                            Map<LineSegment, Result> results = new HashMap<>(Maps.mapCapacity(512));
                            reader.read(splitter, columns -> {
                                LineSegment station = (LineSegment) columns.get(0);
                                int dec = columns.getInt(1);
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

    private static int parseValue(LineSegment segment) {
        int value = 0;
        int pos = 1;
        long head = segment.bytesAt(0, 5);
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
            if (count == 0) {
                return coll;
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
