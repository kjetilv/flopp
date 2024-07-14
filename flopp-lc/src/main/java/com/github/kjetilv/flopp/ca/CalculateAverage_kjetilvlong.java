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
import com.github.kjetilv.flopp.kernel.util.LineSegmentHashtable;
import com.github.kjetilv.flopp.kernel.util.LineSegmentMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class CalculateAverage_kjetilvlong {

    public static void main(String[] args) throws IOException {
        Path path = resolve(Path.of(args[0]));
        Settings settings = new Settings(
            1,
            64,
            .5d,
            .001d,
            .0001d
        );
        long size = Files.size(path);
        int cpus = Runtime.getRuntime().availableProcessors();
        System.out.println(
            size + " bytes on " + cpus + " cpus: " + settings
        );
        System.out.println(
            "  " + partitioning(cpus, Shape.of(path), settings)
        );
        Map<String, Result> map = go(path, settings);

        if (args.length > 1) {
            String content = Files.readString(resolve(Path.of(args[1])));
            String string = map.toString();
            System.out.println(string.equals(content.trim()));
        }
    }

    @SuppressWarnings({"unused", "ResultOfMethodCallIgnored"})
    static Map<String, Result> go(Path path, Settings settings) {
        Instant start = Instant.now();
        Shape shape = Shape.of(path, UTF_8).longestLine(128);
        int cpus = Runtime.getRuntime().availableProcessors();
        CsvFormat format = new CsvFormat.Simple(2, ';');
        Partitioning p = partitioning(cpus, shape, settings);
        Partitions partitions = p.of(shape.size());
        AtomicInteger threads = new AtomicInteger();
        Map<String, Result> map = new TreeMap<>();
        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path, p, shape);
            ExecutorService executor = new ThreadPoolExecutor(
                cpus,
                cpus,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(bitwisePartitioned.partitions().size())
            )
        ) {
            int chunks = bitwisePartitioned.partitions().size();
            BlockingQueue<Map<String, Result>> queue = new ArrayBlockingQueue<>(chunks);
            Map<Long, LineSegmentHashtable<Result>> rs = new ConcurrentHashMap<>();
            bitwisePartitioned.splitters(format, executor)
                .forEach(future ->
                    future.thenApply(CalculateAverage_kjetilvlong::table)
                        .thenAccept(table ->
                            queue.offer(table.toStringMap())));
            for (int i = 0; i < chunks; i++) {
                merge(map, take(queue));
            }
        }
        System.out.println(map);
        System.out.println(Duration.between(start, Instant.now()));
        return map;
    }

    private static Path resolve(Path path) {
        return Optional.of(path)
            .filter(Files::isRegularFile)
            .or(() ->
                Optional.of(path.toString())
                    .filter(str ->
                        str.contains("~"))
                    .map(str ->
                        str.replace("~", System.getProperty("user.home")))
                    .map(Path::of))
            .filter(Files::isRegularFile)
            .map(Path::normalize)
            .orElseThrow(() ->
                new IllegalArgumentException("No path: " + path));
    }

    private static Partitioning partitioning(int cpus, Shape shape, Settings settings) {
        Partitioning basic = Partitioning.create(cpus * settings.cpuMultiplier(), shape.longestLine());
        if (shape.size() < 1_000_000) {
            return basic;
        }
        return basic.fragment(
            cpus * settings.tailMultiplier(),
            settings.tailPerc(),
            settings.partitionMaxPerc(),
            settings.partitionMinPerc()
        );
    }

    private static LineSegmentMap<Result> table(PartitionedSplitter splitter) {
        LineSegmentMap<Result> table = new LineSegmentHashtable<>(32 * 1024);
        Reader reader = Readers.create(
            Column.ofInt(1, CalculateAverage_kjetilvlong::parseValue)
        );
        reader.read(splitter, columns -> {
            LineSegment segment = columns.getRaw(0);
            Result result = table.get(segment, Result::new);
            result.add(columns.getInt(1));
        });
        return table;
    }

    private static int parseValue(LineSegment segment) {
        int value = 0;
        int pos = 1;
        long head = segment.bytesAt(0, 5);
        int len = (int) segment.length();
        for (int i = len - 1; i >= 0; i--) {
            int shift = i * 8;
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

    private static <K> void merge(Map<K, Result> acc, Map<K, Result> map) {
        map.forEach((key, value) ->
            acc.merge(key, value, Result::merge));
    }

    private static <T> T take(BlockingQueue<T> queue) {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Failed", e);
        }
    }

    public record Settings(
        int cpuMultiplier,
        int tailMultiplier,
        double tailPerc,
        double partitionMaxPerc,
        double partitionMinPerc
    ) {
    }

    public static final class Result {

        private int count;

        private int min;

        private int max;

        private int sum;

        private Result() {
            this.min = Integer.MAX_VALUE;
            this.max = Integer.MIN_VALUE;
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

        void add(int value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        private static double round(double value) {
            return Math.round(value) / 10.0;
        }
    }
}
