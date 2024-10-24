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

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.PartitionedProcessor;
import com.github.kjetilv.flopp.kernel.PartitionedSplitter;
import com.github.kjetilv.flopp.kernel.columns.Column;
import com.github.kjetilv.flopp.kernel.columns.ColumnReader;
import com.github.kjetilv.flopp.kernel.columns.ColumnReaders;
import com.github.kjetilv.flopp.kernel.files.PartitionedPaths;
import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.formats.Formats;
import com.github.kjetilv.flopp.kernel.formats.Shape;
import com.github.kjetilv.flopp.kernel.partitions.Partitioning;
import com.github.kjetilv.flopp.kernel.segments.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("preview")
public final class CalculateAverage_kjetilvlong {

    public static void main(String[] args) throws IOException {
        Path inputFile = pathArgument(args, 0)
            .filter(Files::isRegularFile)
            .orElseThrow(() ->
                new IllegalArgumentException("No path: " + args[0]));
        Optional<Path> truthFile = pathArgument(args, 1).filter(Files::isRegularFile);
        Optional<Path> outFile = pathArgument(args, 2);

        Settings settings = new Settings(
            1,
            64,
            .5d,
            .001d,
            .0001d
        );
        long size = Files.size(inputFile);
        int cpus = Runtime.getRuntime().availableProcessors();
        System.out.println(
            size + " bytes on " + cpus + " cpus: " + settings
        );
        System.out.println(
            "  " + readPartitioning(cpus, Shape.of(inputFile), settings)
        );

        Instant start = Instant.now();
        Format.Csv simple = Formats.Csv.simple(2, ';');

//        Result result = mapAverage(inputFile, settings, simple);
//        System.out.println(result);
//        System.out.println(Duration.between(start, Instant.now()));
//
//        start = Instant.now();

        LineSegmentMap<Result> map = mapAverages(inputFile, settings, simple);
        String mapStringSorted = map.toStringSorted();
        System.out.println(mapStringSorted);
        System.out.println(Duration.between(start, Instant.now()));

        truthFile
            .map(CalculateAverage_kjetilvlong::readString)
                .ifPresent(contents ->
                System.out.println(mapStringSorted.equals(contents.trim())));

        outFile.ifPresent(out ->
            temper(
                map,
                inputFile,
                settings,
                simple,
                out
            ));
    }

    static Result mapAverage(Path path, Settings settings, Format.Csv format) {
        Shape shape = Shape.of(path, UTF_8).longestLine(128);
        int cpus = Runtime.getRuntime().availableProcessors();
        Partitioning p = readPartitioning(cpus, shape, settings);
        try (
            Partitioned<Path> partitioned = PartitionedPaths.partitioned(path, p, shape)
        ) {
            int chunks = partitioned.partitions().size();
            BlockingQueue<Result> queue = new ArrayBlockingQueue<>(chunks);
            Function<Throwable, Boolean> printException = CalculateAverage_kjetilvlong::print;
            partitioned.splitters(format)
                .map(splitter ->
                    CompletableFuture.supplyAsync(() -> result(splitter))
                        .thenApply(queue::offer)
                        .exceptionally(printException))
                .map(CompletableFuture::join)
                .reduce((b1, b2) -> b1 & b2)
                .orElse(false);
            Result result = new Result();
            for (int i = 0; i < chunks; i++) {
                result.merge(take(queue));
            }
            return result;
        }
    }

    static LineSegmentMap<Result> mapAverages(
        Path path,
        Settings settings,
        Format.Csv format
    ) {
        Shape shape = Shape.of(path, UTF_8).longestLine(128);
        Partitioning partitioning = readPartitioning(
            Runtime.getRuntime().availableProcessors(),
            shape,
            settings
        );
        int size = 32 * 1024;
        LineSegmentMap<Result> map = LineSegmentMaps.create(size);
        try (
            Partitioned<Path> partitioned = PartitionedPaths.partitioned(path, partitioning, shape);
            StructuredTaskScope<Boolean> scope = new StructuredTaskScope<>()
        ) {
            int partitionCount = partitioned.partitions().size();
            BlockingQueue<LineSegmentMap<Result>> queue = new ArrayBlockingQueue<>(partitionCount);
            partitioned.splitters(format)
                .forEach(splitter ->
                    scope.fork(() -> queue.offer(table(splitter, size)))
                );
            for (int i = 0; i < partitionCount; i++) {
                map.merge(take(queue), Result::merge);
            }
            scope.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return map;
    }

    static void temper(
        LineSegmentMap<Result> map,
        Path originalPath,
        Settings settings,
        Format.Csv format,
        Path out
    ) {
        Shape shape = Shape.of(originalPath, UTF_8).longestLine(128);
        int cpus = Runtime.getRuntime().availableProcessors();
        Partitioning p = readPartitioning(cpus, shape, settings);
        try (
            Partitioned<Path> partitioned = PartitionedPaths.partitioned(originalPath, p, shape);
            PartitionedProcessor<Path, SeparatedLine, Stream<LineSegment>> processor = partitioned.processTo(
                out,
                format
            )
        ) {
            processor.forEachPartition(() -> {
                    LineSegmentMap<Result> copy = map.freeze();
                    return separatedLine -> {
                        LineSegment city = separatedLine.segment(0);
                        Result result = copy.get(city);
                        String cityString = city.asString();
                        String measurement = separatedLine.segment(1).asString();
                        String formatted = "%s;%s;%s;%d;%s\n"
                            .formatted(
                                cityString,
                                measurement,
                                result.min / 10.0,
                                result.perc(measurement),
                                result.max / 10.0
                            );
                        return Stream.of(LineSegments.of(formatted));
                    };
                }
            );
        }
    }

    private static Optional<Path> pathArgument(String[] args, int no) {
        return Arrays.stream(args)
            .skip(no)
            .findFirst()
            .flatMap(path ->
                Optional.of(path)
                    .filter(str ->
                        str.contains("~"))
                    .map(str ->
                        str.replace("~", System.getProperty("user.home")))
                            .map(Path::of)
                    .or(() ->
                        Optional.of(Path.of(path)))
                    .map(Path::normalize));
    }

    private static boolean print(Throwable throwable) {
        System.out.println("Error: " + throwable.getMessage());
        for (
            Throwable cause = throwable.getCause();
            cause != null && cause != cause.getCause();
            cause = cause.getCause()
        ) {
            System.out.println("  Cause: " + cause);
        }
        return false;
    }

    private static Partitioning readPartitioning(int cpus, Shape shape, Settings settings) {
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

    @SuppressWarnings("unused")
    private static Partitioning writePartitioning(int cpus, Shape shape, Settings settings) {
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
        LineSegmentMap<Result> table = LineSegmentMaps.create(size);
        ColumnReader columnReader = ColumnReaders.create(
            Column.ofInt(1, CalculateAverage_kjetilvlong::parseValue)
        );
        columnReader.read(
            splitter, columns -> {
                LineSegment segment = columns.getRaw(0);
                Result result = table.get(segment, Result::new);
                result.add(columns.getInt(1));
            }
        );
        return table;
    }

    private static Result result(PartitionedSplitter splitter) {
        Result result = new Result();
        ColumnReader columnReader = ColumnReaders.create(
            Column.ofInt(1, CalculateAverage_kjetilvlong::parseValue)
        );
        columnReader.read(
            splitter, columns -> {
                LineSegment segment = columns.getRaw(0);
                result.add(columns.getInt(1));
            }
        );
        return result;
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

    private static String readString(Path file) {
        try {
            return Files.readString(file);
        } catch (Exception e) {
            throw new IllegalStateException(e);
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

        public int perc(String measurement) {
            double m = new BigDecimal(measurement).multiply(BigDecimal.TEN).doubleValue();
            int range = max - min;
            double normalizedM = m - min;
            return Math.toIntExact(Math.round(100 * normalizedM / range));
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
