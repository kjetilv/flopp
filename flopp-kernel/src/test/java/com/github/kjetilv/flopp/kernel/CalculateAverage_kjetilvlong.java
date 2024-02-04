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

import com.github.kjetilv.flopp.kernel.bits.BitwisePartitioned;
import com.github.kjetilv.flopp.kernel.bits.LineSegment;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CalculateAverage_kjetilvlong {

    public static void main(String[] args) {
        Instant start = Instant.now();
        Path path = Path.of(FILE);
        Shape shape = Shape.of(path).longestLine(128);
        Partitioning partitioning = Partitioning.create(
            Runtime.getRuntime().availableProcessors(),
            shape.longestLine()
        ).scaled(2);
        int chunks = partitioning.of(shape.size()).size();
        try (
            ExecutorService executor = new ForkJoinPool(chunks * 2);
            Partitioned<Path> bitwisePartitioned = new BitwisePartitioned(path, partitioning, shape);
            PartitionedStreams streamers = bitwisePartitioned.streams()
        ) {
            System.out.println(Duration.between(start, Instant.now()));
            List<? extends PartitionStreamer> partitionStreamers = streamers.streamersList();
            List<CompletableFuture<Map<String, Result>>> list = partitionStreamers
                .stream()
                .map(streamer ->
                    CompletableFuture.supplyAsync(streamer::lines, executor).thenApplyAsync(
                        CalculateAverage_kjetilvlong::toMap,
                        executor
                    ))
                .toList();
            System.out.println(Duration.between(start, Instant.now()));

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

    private static final String FILE = "./measurements.txt";

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

    private static Map<String, Result> combineMaps(
        Set<String> keys,
        List<Map<String, Result>> maps
    ) {
        TreeMap<String, Result> treeMap = new TreeMap<>(maps.getFirst());
        maps.stream().skip(1)
            .forEach(map -> {
                keys.forEach(key -> {
                    Result base = treeMap.get(key);
                    Result addendum = map.get(key);
                    if (base == null) {
                        treeMap.put(key, addendum);
                    } else if (addendum != null) {
                        Result merged = base.merge(addendum);
                        treeMap.put(key, merged);
                    }
                });
            });
        return treeMap;
    }

    private static int semiIndex(LineSegment ls, long length) {
        for (int i = Math.toIntExact(length - 3); i >= 0; i--) {
            if (ls.byteAt(i) == ';') {
                return i;
            }
        }
        throw new IllegalStateException(STR."No split in \{ls.asString()}");
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

    private static final class Result {

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
            return STR."\{round(min)}/\{round(1.0 * sum / count)}/\{round(max)}";
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
