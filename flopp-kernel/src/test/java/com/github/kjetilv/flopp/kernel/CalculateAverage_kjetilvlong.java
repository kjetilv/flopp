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

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("StringTemplateMigration")
public final class CalculateAverage_kjetilvlong {

    public static void main(String[] args) {
        for (String arg : args) {
            Path path = Path.of(arg);
            go2(path);
//            go1(path);
        }
    }

    public static Map<String, Result> go(
        Path path,
        Partitioning partitioning,
        Shape shape
    ) {
        return go(path, partitioning, shape, false);
    }

    public static Map<String, Result> go(
        Path path,
        Partitioning partitioning,
        Shape shape,
        boolean slow
    ) {
        return go(path, partitioning, shape, slow, null);
    }

    public static Map<String, Result> go(
        Path path,
        Partitioning partitioning,
        Shape shape,
        boolean slow,
        Consumer<SeparatedLine> callbacks
    ) {
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
            List<Supplier<Map<String, Result>>> mapSuppliers =
                bitwisePartitioned.csvSplitters()
                    .splitters(new CsvFormat(';'))
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
            return combineMaps(keySet(maps), maps);
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

    private static void go2(Path path) {
        Instant start = Instant.now();
        Shape shape = Shape.of(path).longestLine(64);
        Map<String, Result> go = go(
            path,
            Partitioning.create(
                Runtime.getRuntime().availableProcessors(),
                shape.longestLine()
            ).scaled(2), shape
        );
        System.out.println(go);
        System.out.println(Duration.between(start, Instant.now()));
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

    private static Set<String> keySet(List<Map<String, Result>> maps) {
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

    private static SequencedMap<String, Result> combineMaps(
        Set<String> keys,
        List<Map<String, Result>> maps
    ) {
        SequencedMap<String, Result> treeMap = new TreeMap<>(maps.getFirst());
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

    private static int semiIndex(LineSegment ls, long length) {
        for (int i = Math.toIntExact(length - 3); i >= 0; i--) {
            if (ls.byteAt(i) == ';') {
                return i;
            }
        }
        throw new IllegalStateException("No split in " + ls.asString());
    }

    private static int parseValue(LineSegment ls) {
        int value = 0;
        int pos = 1;
        long length = ls.length();
        for (long i = 0; i < length; i++) {
            byte b = ls.byteAt(length - i - 1);
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
