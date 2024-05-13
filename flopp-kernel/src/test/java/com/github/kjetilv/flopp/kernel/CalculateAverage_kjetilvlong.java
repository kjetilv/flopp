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
package com.github.kjetilv.flopp.kernel;

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
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CalculateAverage_kjetilvlong {

    public static void main(String[] args) {
        for (String arg : args) {
            Path path = Path.of(arg);
//            go(path);
            go3(path);
//            go3(path);
//            go3(path);
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
        Shape shape = Shape.of(path).longestLine(128);
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
            List<Supplier<Map<String, Result>>> mapSuppliers =
                bitwisePartitioned.splitters()
                    .splitters(new CsvFormat.Escaped(';', '\\'))
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
            SequencedMap<String, Result> combinedMaps = combineMaps(keySet(maps), maps);
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
    private static void go1(Path path) {
        Instant start = Instant.now();
        Shape shape = Shape.of(path).longestLine(128);
        Partitioning partitioning = Partitioning.create(
            Runtime.getRuntime().availableProcessors(),
            shape.longestLine()
        ).scaled(2);
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
            List<? extends PartitionStreamer> partitionStreamers =
                bitwisePartitioned.streams().streamersList();
            List<CompletableFuture<Map<String, Result>>> list = partitionStreamers
                .stream()
                .map(streamer ->
                    CompletableFuture.supplyAsync(streamer::lines)
                        .thenApplyAsync(CalculateAverage_kjetilvlong::toMap, executor))
                .toList();
            List<Map<String, Result>> maps = list.stream()
                .map(CompletableFuture::join)
                .toList();
            System.out.println(Duration.between(start, Instant.now()));
            Set<String> keys = keySet(maps);
            Map<String, Result> map = combineMaps(keys, maps);
            System.out.println(map);
            System.out.println(Duration.between(start, Instant.now()));
        }
    }

    @SuppressWarnings("unused")
    private static void go3(Path path) {
        Instant start = Instant.now();
        Shape shape = Shape.of(path).longestLine(128);
        Partitioning partitioning = Partitioning.create(
            Runtime.getRuntime().availableProcessors(),
            shape.longestLine())
            .scaled(2);
        CsvFormat format = new CsvFormat.Simple(';', 2);
        Reader reader = Readers.create(
            Column.ofString("station", 1),
            Column.ofType("measurement", 2, CalculateAverage_kjetilvlong::parseValue)
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
            Stream<PartitionedSplitter> partitionStreamers = bitwisePartitioned.splitters().splitters(format);
            List<CompletableFuture<Map<String, Result>>> list = partitionStreamers
                .map(splitter ->
                    CompletableFuture.supplyAsync(
                        () -> {
                            Map<String, Result> m = Maps.ofSize(512);
                            reader.read(splitter, columns ->
                                m.compute(
                                    (String) columns.get("station"),
                                    (station, existing) -> {
                                        int dec = (Integer) columns.get("measurement");
                                        return existing == null ? new Result(dec) : existing.collect(dec);
                                    }
                                ));
                            return m;
                        },
                        executor
                    ))
                .toList();
            List<Map<String, Result>> maps = list.stream()
                .map(CompletableFuture::join)
                .toList();
            System.out.println(Duration.between(start, Instant.now()));
            Set<String> keys = keySet(maps);
            Map<String, Result> map = combineMaps(keys, maps);
            System.out.println(map);
            System.out.println(Duration.between(start, Instant.now()));
        }
    }

    @SuppressWarnings("unused")
    private static void go4It(Path path) {
        Instant start = Instant.now();
        Shape shape = Shape.of(path).longestLine(128);
        Partitioning partitioning = Partitioning.create(
            Runtime.getRuntime().availableProcessors(),
            shape.longestLine()
        ).scaled(2);
        CsvFormat format = new CsvFormat.Simple(';', 2);
        Reader reader = Readers.create(
            Column.ofBinary("station", 1),
            Column.ofType("measurement", 2, CalculateAverage_kjetilvlong::parseValue)
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
            Stream<PartitionedSplitter> partitionStreamers = bitwisePartitioned.splitters().splitters(format);
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

//    private static void go2(Path path) {
//        Instant start = Instant.now();
//        Shape shape = Shape.of(path).longestLine(64);
//        Map<String, Result> go = go(
//            path,
//            Partitioning.create(
//                Runtime.getRuntime().availableProcessors(),
//                shape.longestLine()
//            ).scaled(2), shape
//        );
//        System.out.println(go);
//        System.out.println(Duration.between(start, Instant.now()));
//    }

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
            String station = separatedLine.column(0);
            int measure = parseValue(separatedLine.segment(1));
            m.compute(
                station,
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

    private static Map<String, Result> toMap(Stream<LineSegment> lines) {
        try (lines) {
            Map<String, Result> m = new HashMap<>(1024, 1.0f);
            lines.forEach(ls -> {
                long length = ls.length(); //segment.length();
                int splitIndex = semiIndex(ls, length);
                int value = parseValue(
                    Math.toIntExact(length),
                    splitIndex,
                    ls
                );
                String key = ls.asString(splitIndex);
                m.compute(
                    key,
                    (_, existing) ->
                        existing == null
                            ? new Result(value)
                            : existing.collect(value)
                );
            });
            return m;
        }
    }

    private static <T extends Comparable<T>> SequencedMap<T, Result> combineMaps(
        Set<T> keys,
        List<Map<T, Result>> maps
    ) {
        SequencedMap<T, Result> treeMap = new TreeMap<>(maps.getFirst());
        maps.stream().skip(1)
            .forEach(map ->
                keys.forEach(key -> {
                    Result base = treeMap.get(key);
                    Result addendum = map.get(key);
                    if (base == null) {
                        treeMap.put(key, addendum);
                    } else if (addendum != null) {
                        Result merged = base.merge(addendum);
                        treeMap.put(key, merged);
                    }
                }));
        return treeMap;
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
        map.forEach((segment, result) -> seqMap.put(segment.asString(), result));
        return seqMap;
    }

    private static int semiIndex(LineSegment ls, long length) {
        for (int i = Math.toIntExact(length - 3); i >= 0; i--) {
            if (ls.byteAt(i) == ';') {
                return i;
            }
        }
        throw new IllegalStateException("No split in " + ls.asString());
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
        long value = 0;
        long pos = 1;
        long head = segment.head();
        int intExact = Math.toIntExact(segment.length());
        for (int i = intExact - 1; i >= 0; i--) {
            int shift = i * 8;
            long b = head >> shift & 0xFF;
            if (b == '.') {
                continue;
            }
            if (b == '-') {
                return (int) value * -1;
            }
            long j = b - '0';
            value += j * pos;
            pos *= 10;
        }
        return (int) value;
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
