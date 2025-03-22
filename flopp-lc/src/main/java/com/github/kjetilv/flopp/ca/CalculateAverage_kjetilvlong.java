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
import com.github.kjetilv.flopp.kernel.columns.Column;
import com.github.kjetilv.flopp.kernel.columns.ColumnReader;
import com.github.kjetilv.flopp.kernel.columns.ColumnReaders;
import com.github.kjetilv.flopp.kernel.files.PartitionedPaths;
import com.github.kjetilv.flopp.kernel.formats.Formats;
import com.github.kjetilv.flopp.kernel.partitions.Partitioning;
import com.github.kjetilv.flopp.kernel.partitions.Partitionings;
import com.github.kjetilv.flopp.kernel.segments.LineSegmentMap;
import com.github.kjetilv.flopp.kernel.segments.LineSegmentMaps;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
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
//        Optional<Path> truthFile = pathArgument(args, 1).filter(Files::isRegularFile);
//        Optional<Path> outFile = pathArgument(args, 2);

        Settings settings = new Settings(3, 8, 6, 7, 8);
        long size = Files.size(inputFile);
        int cpus = Runtime.getRuntime().availableProcessors();
        System.out.println(size + " bytes on " + cpus + " cpus: " + settings);

        Instant start = Instant.now();
        Format.Csv simple = Formats.Csv.simple(2, (byte) ';');

//        Result result = mapAverage(inputFile, settings, simple);
//        System.out.println(result);
//        System.out.println(Duration.between(start, Instant.now()));
//
//        start = Instant.now();

        LineSegmentMap<Result> map = mapAverages(inputFile, settings, simple);
        String mapStringSorted = map.toStringSorted();
        System.out.println(mapStringSorted);
        System.out.println(Duration.between(start, Instant.now()));

//        truthFile
//            .map(CalculateAverage_kjetilvlong::readString)
//                .ifPresent(contents ->
//                System.out.println(mapStringSorted.equals(contents.trim())));
//
//        outFile.ifPresent(out ->
//            temper(
//                map,
//                inputFile,
//                settings,
//                simple,
//                out
//            ));
    }

    static Result mapAverage(Path path, Settings settings, Format.Csv format) {
        Shape shape = Shape.of(path, UTF_8).longestLine(128);
        int cpus = Runtime.getRuntime().availableProcessors();
        Partitioning p = readPartitioning(cpus, shape, settings);
        try (
            Partitioned partitioned = PartitionedPaths.partitioned(path, p, shape)
        ) {
            int chunks = partitioned.partitions().size();
            BlockingQueue<Result> queue = new ArrayBlockingQueue<>(chunks);
            Function<Throwable, Boolean> printException = CalculateAverage_kjetilvlong::print;
            Boolean completed = partitioned.splitters(format)
                .map(splitter ->
                    CompletableFuture.supplyAsync(() -> result(splitter))
                        .thenApply(queue::offer)
                        .exceptionally(printException))
                .map(CompletableFuture::join)
                .reduce((b1, b2) -> b1 & b2)
                .orElse(false);
            Result result = new Result();
            for (int i = 0; i < chunks; i++) {
                Result take = de(queue);
                if (take == null) {
                    throw new IllegalStateException("Failed to retrieve result #" + (i + 1) + "/" + chunks);
                }
                result.merge(take);

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
        BlockingQueue<LineSegmentMap<Result>> queue;
        int partitionCount;
        try (
            Partitioned partitioned = PartitionedPaths.vectorPartitioned(path, partitioning, shape);
            StructuredTaskScope<Boolean> scope = new StructuredTaskScope<>()
        ) {
            partitionCount = partitioned.partitions().size();
            queue = new ArrayBlockingQueue<>(partitionCount);
            List<StructuredTaskScope.Subtask<Boolean>> subtasks = partitioned.splitters(format)
                .map(splitter ->
                    scope.fork(() ->
                        queue.offer(table(splitter, size))))
                .toList();
            scope.fork(() ->
                merge(partitionCount, map, queue));
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
            PartitionedProcessors<Path> partitioned = PartitionedPaths.partitionedProcessors(originalPath, p, shape);
            PartitionedProcessor<SeparatedLine, Stream<LineSegment>> processor =
                partitioned.processTo(out, format)
        ) {
            processor.forEachPartition(() -> {
                    LineSegmentMap<Result> copy = map.freeze();
                    return separatedLine -> {
                        LineSegment city = separatedLine.segment(0);
                        Result result = copy.get(city);
                        String cityString = city.asTerminatedString();
                        String measurement = separatedLine.segment(1).asTerminatedString();
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

    private static final Partitionings PARTITIONINGS = Partitionings.LONG;

    private static boolean merge(
        int partitionCount,
        LineSegmentMap<Result> map,
        BlockingQueue<LineSegmentMap<Result>> queue
    ) {
        for (int i = 0; i < partitionCount; i++) {
            LineSegmentMap<Result> result = de(queue);
            if (result == null) {
                throw new IllegalStateException("Failed to retrieve result #" + (i + 1) + "/" + partitionCount);
            }
            map.merge(result, Result::merge);
        }
        return true;
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
        Partitioning basic = PARTITIONINGS.create(cpus * settings.cpuMultiplier(), shape.longestLine());
        if (shape.size() < 1_000_000) {
            return basic;
        }
        TailShards tailShards = tailShards(cpus, settings);
        return basic.fragment(tailShards);
    }

    @SuppressWarnings("unused")
    private static Partitioning writePartitioning(int cpus, Shape shape, Settings settings) {
        Partitioning basic = PARTITIONINGS.create(cpus * settings.cpuMultiplier(), shape.longestLine());
        if (shape.size() < 1_000_000) {
            return basic;
        }
        return basic.fragment(tailShards(cpus, settings));
    }

    private static TailShards tailShards(int cpus, Settings settings) {
        return new TailShards(
            cpus * settings.tailMultiplier(),
            settings.tailDim(),
            settings.maxDim(),
            settings.minDim()
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

    private static <T> T de(BlockingQueue<T> queue) {
        try {
            return queue.poll(3, TimeUnit.SECONDS);
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
        int tailDim,
        int maxDim,
        int minDim
    ) {

        public Settings {
            if (!(tailDim <= maxDim && maxDim <= minDim)) {
                throw new IllegalStateException(this + " has wrong sizes");
            }
        }
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

        public String toString() {
            return round(min) + "/" + round(1.0 * sum / count) + "/" + round(max);
        }
    }
}
