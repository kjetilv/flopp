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
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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

        Instant start = Instant.now();
        LineSegmentMap<Result> map = go(path, settings, new CsvFormat.Simple(2, ';'));
        System.out.println(map.toStringSorted());
        System.out.println(Duration.between(start, Instant.now()));

        if (args.length > 1) {
            String content = Files.readString(resolve(Path.of(args[1])));
            String string = map.toString();
            System.out.println(string.equals(content.trim()));
        }
    }

    @SuppressWarnings({"unused", "ResultOfMethodCallIgnored"})
    static LineSegmentMap<Result> go(Path path, Settings settings, CsvFormat format) {
        Shape shape = Shape.of(path, UTF_8).longestLine(128);
        int cpus = Runtime.getRuntime().availableProcessors();
        Partitioning p = partitioning(cpus, shape, settings);
        Partitions partitions = p.of(shape.size());
        AtomicInteger threads = new AtomicInteger();
        int size = 32 * 1024;
        LineSegmentHashtable<Result> map = new LineSegmentHashtable<>(size);
        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path, p, shape);
            ExecutorService workingExecutor = new ThreadPoolExecutor(
                cpus,
                cpus,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(bitwisePartitioned.partitions().size())
            )
        ) {
            int chunks = bitwisePartitioned.partitions().size();
            BlockingQueue<LineSegmentMap<Result>> queue = new ArrayBlockingQueue<>(chunks);
            Function<Throwable, Void> printException = CalculateAverage_kjetilvlong::print;
            bitwisePartitioned.splitters(format, workingExecutor)
                .forEach(future ->
                    future
                        .thenApply(splitter -> table(splitter, size))
                        .thenAccept(queue::offer)
                        .exceptionally(printException));
            for (int i = 0; i < chunks; i++) {
                map.merge(take(queue), Result::merge);
            }
            return map;
        }
    }

    private static Void print(Throwable throwable) {
        System.out.println("Error: " + throwable.getMessage());
        for (
            Throwable cause = throwable.getCause();
            cause != null && cause != cause.getCause();
            cause = cause.getCause()
        ) {
            System.out.println("  Cause: " + cause);
        }
        return null;
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

    private static LineSegmentMap<Result> table(PartitionedSplitter splitter, int size) {
        LineSegmentMap<Result> table = new LineSegmentHashtable<>(size);
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
