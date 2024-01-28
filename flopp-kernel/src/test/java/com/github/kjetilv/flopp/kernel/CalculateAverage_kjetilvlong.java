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

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public final class CalculateAverage_kjetilvlong {

    public static void main(String[] args) {
//        runSpull();
        runJava();
    }

    private static final String FILE = "./measurements.txt";

    private static void runJava() {
        Instant start = Instant.now();
        Path path = Path.of(FILE);
        Shape shape = Shape.of(path).longestLine(64);
        Partitioning partitioning = Partitioning.longAligned(
            Runtime.getRuntime().availableProcessors(),
            shape.stats().longestLine() + 2
        );
        BitwisePartitioned bitwisePartitioned = new BitwisePartitioned(path, partitioning, shape);
        try (
            BitwisePartitionStreamers streamers = bitwisePartitioned.streamers();
        ) {
            System.out.println(Duration.between(start, Instant.now()));
            List<CompletableFuture<Map<String, Result>>> list = streamers.streamers()
                .map(streamer ->
                    CompletableFuture.supplyAsync(
                        streamer::lines,
                        Executors.newVirtualThreadPerTaskExecutor()
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
                .forEach(seg -> {
                    long length = seg.length(); //segment.length();
                    byte[] bytes = LineSegments.toBytes(seg);
                    int splitIndex = -1;
                    for (int i = Math.toIntExact(length - 3); i >= 0; i--) {
                        if (bytes[i] == ';') {
                            splitIndex = i;
                            break;
                        }
                    }
                    int value = parseValue(Math.toIntExact(length), splitIndex, bytes);
                    String key = new String(bytes, 0, splitIndex);
                    m.compute(key, (_, result) ->
                        result == null ? new Result(value) : result.collect(value));
                });
            return m;
        }
    }

    private static int parseValue(int length, int splitIndex, byte[] bytes) {
        int value = 0;
        int boundary = splitIndex + 1;
        int pos = 1;
        for (int i = length - 1; i >= boundary; i--) {
            byte b = bytes[i];
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
