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
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class CalculateAverage_kjetilvlong {

    public static void main(String[] args) throws IOException {
        Path path = Path.of(args[0]);
        Map<String, Result> stringResultMap = go6(path, null);

        if (args.length > 1) {
            String content = Files.readString(Path.of(args[1]));
            System.out.println(stringResultMap.toString().equals(content.trim()));
        }
    }

    @SuppressWarnings({"unused", "ResultOfMethodCallIgnored"})
    static Map<String, Result> go6(Path path, Partitioning partitioning) {
        Instant start = Instant.now();
        Shape shape = Shape.of(path, UTF_8).longestLine(128);
        int cpus = Runtime.getRuntime().availableProcessors();
        CsvFormat format = new CsvFormat.Simple(2, ';');
        Partitioning p = partitioning == null ? partitioning(cpus, shape) : partitioning;
        int chunks = p.of(shape.size()).size();
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
            BlockingQueue<Map<String, Result>> queue = new ArrayBlockingQueue<>(chunks);
            bitwisePartitioned.splitters(format, executor)
                .forEach(splitterFuture ->
                    splitterFuture.thenAccept(splitter ->
                        queue.offer(table(splitter).toStringMap())));
            for (int i = 0; i < chunks; i++) {
                merge(map, take(queue));
            }
        }
        System.out.println(map);
        System.out.println(Duration.between(start, Instant.now()));
        return map;
    }

    private static Map<String, Result> take(BlockingQueue<Map<String, Result>> queue) {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Failed", e);
        }
    }

    private CalculateAverage_kjetilvlong() {
    }

    private static void merge(Map<String, Result> acc, Map<String, Result> map) {
        map.forEach((key, value) ->
            acc.merge(key, value, Result::merge));
    }

    private static LineSegmentMap<Result> table(PartitionedSplitter splitter) {
        LineSegmentMap<Result> table = new LineSegmentHashtable<>(24 * 1024);
        Reader reader = Readers.create(
            Column.ofInt(1, CalculateAverage_kjetilvlong::parseValue)
        );
        reader.read(splitter, columns -> {
            LineSegment raw = columns.getRaw(0);
            Result result = table.get(raw, Result::new);
            result.add(columns.getInt(1));
        });
        return table;
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
