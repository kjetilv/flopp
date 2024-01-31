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

import com.github.kjetilv.flopp.kernel.bits.*;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

public final class CalculateAverage_kjetilvlong {

    public static void main(String[] args) {
        runJava();
    }

    private static final String FILE = "./measurements.txt";

    private static void runJava() {
        Instant start = Instant.now();
        Path path = Path.of(FILE);
        Shape shape = Shape.of(path).longestLine(128);
        Partitioning partitioning = Partitioning.longAligned(
            Runtime.getRuntime().availableProcessors(),
            shape.stats().longestLine()
        ).scaled(2);
        try (
            Partitioned<Path> bitwisePartitioned = new BitwisePartitioned(path, partitioning, shape);
            PartitionedStreams streamers = bitwisePartitioned.streams()
        ) {
            System.out.println(Duration.between(start, Instant.now()));
            List<CompletableFuture<Map<String, Result>>> list = streamers.streamers()
                .map(streamer ->
                    CompletableFuture.supplyAsync(
                        streamer::lines,
                        new ForkJoinPool(Runtime.getRuntime().availableProcessors() * 2)
                    ))
                .map(future ->
                    future.thenApply(
                        CalculateAverage_kjetilvlong::toMap))
                .toList();
            System.out.println(Duration.between(start, Instant.now()));

            List<Map<String, Result>> maps = list.stream()
                .map(CompletableFuture::join)
                .toList();
            System.out.println(Duration.between(start, Instant.now()));
            Map<String, Result> map = maps.stream()
                .<Map<String, Result>>reduce(
                    new TreeMap<>(),
                    (m1, m2) -> {
                        m2.forEach((k, v) ->
                            m1.compute(k, (_, r1) ->
                                r1 == null ? v : r1.merge(v)));
                        return m1;
                    },
                    (_, _) -> null
                );
            System.out.println(map);
            System.out.println(Duration.between(start, Instant.now()));
        }
    }

    private static Map<String, Result> toMap(Stream<LineSegment> lines) {
        try (lines) {
            Map<String, Result> m = new HashMap<>(1024, 1.0f);
            lines
                .forEach(ls -> {
                    long offset = ls.offset();
                    long length = ls.length(); //segment.length();
                    MemorySegment memorySegment = ls.memorySegment();
                    int splitIndex = semiIndex(ls, length);
                    int value = parseValue(offset, Math.toIntExact(length), splitIndex, memorySegment);
                    String key = ls.asString(splitIndex);
                    m.compute(key, (_, result) ->
                        result == null ? new Result(value) : result.collect(value));
                });
            return m;
        }
    }

    private static int semiIndex(LineSegment ls, long length) {
        for (int i = Math.toIntExact(length - 3); i >= 0; i--) {
            if (ls.byteAt(i) == ';') {
                return i;
            }
        }
        throw new IllegalStateException(STR."No split in \{ls.asString()}");
    }

    private static int parseValue(long offset, int length, int splitIndex, MemorySegment memorySegment) {
        int value = 0;
        int boundary = splitIndex + 1;
        int pos = 1;
        for (int i = length - 1; i >= boundary; i--) {
            byte b = memorySegment.get(ValueLayout.JAVA_BYTE, offset + i);
            if (b == '.') {
                continue;
            }
            if (b == '-') {
                value *= -1;
                continue;
            }
            int num = b - '0';
            value += num * pos;
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
            this.sum += value;
            this.count = 1;
        }

        public String toString() {
            return STR."\{round(min)}/\{round(1.0 * sum / count)}/\{round(max)}";
        }

        public Result merge(Result coll) {
            min = Math.min(min, coll.min);
            max = Math.min(max, coll.max);
            sum += coll.sum;
            count += coll.count;
            return this;
        }

        Result collect(int v) {
            min = Math.min(min, v);
            max = Math.max(max, v);
            sum += v;
            count++;
            return this;
        }

        private static double round(double value) {
            return Math.round(value) / 10.0;
        }
    }
}
