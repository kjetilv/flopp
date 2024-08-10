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
import com.github.kjetilv.flopp.kernel.formats.CsvFormat;
import com.github.kjetilv.flopp.kernel.formats.Partitioning;
import com.github.kjetilv.flopp.kernel.formats.Shape;
import com.github.kjetilv.flopp.kernel.readers.Column;
import com.github.kjetilv.flopp.kernel.readers.Readers;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.map.ChronicleMap;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class CalculateAverage_kjetilvcm {

    public static void main(String[] args) {
        for (String arg : args) {
            Path path = Path.of(arg);
            Instant start = Instant.now();
            Shape shape = Shape.of(path, UTF_8).longestLine(128);
            Partitioning partitioning = Partitioning.create(500, shape.longestLine());
            CsvFormat format = CsvFormat.simple(2, ';');
            try (
                Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path, partitioning, shape);
                ExecutorService executor =Executors.newSingleThreadExecutor()
//                    Executors.newVirtualThreadPerTaskExecutor()
            ) {
                Map<String, Result> done = new TreeMap<>();
                try (
                    ChronicleMap<LineSegment, Result> measurements = mapCompletableFuture.join()
                ) {
                    bitwisePartitioned.splitters(format).parallel()
                        .map(splitter ->
                            CompletableFuture.supplyAsync(() ->
                                map(splitter, measurements), executor))
                        .toList()
                        .forEach(CompletableFuture::join);
                    measurements.forEach((ls, res) ->
                        done.put(ls.asString(UTF_8), res.freeze()));
                }
                System.out.println(done);
                System.out.println(Duration.between(start, Instant.now()));
            }
        }
    }

    private CalculateAverage_kjetilvcm() {
    }

    private static final CompletableFuture<ChronicleMap<LineSegment, Result>> mapCompletableFuture =
        CompletableFuture.supplyAsync(
            CalculateAverage_kjetilvcm::getLineSegmentResultChronicleMap,
            ForkJoinPool.commonPool()
        );

    private static ChronicleMap<LineSegment, Result> getLineSegmentResultChronicleMap() {
        return ChronicleMap.of(LineSegment.class, Result.class)
            .keyMarshallers(
                new LineSegmentBytesReader(),
                new LineSegmentBytesWriter()
            )
            .valueMarshallers(
                new ResultBytesReader(),
                new ResultBytesWriter()
            )
            .entries(500)
            .averageKey(LineSegments.of("1234567", UTF_8))
            .averageValue(new Result())
            .create();
    }

    private static Map<LineSegment, Result> map(PartitionedSplitter splitter, Map<LineSegment, Result> m) {
        Readers.create(
            Column.ofSegment("station", 0),
            Column.ofInt("measurement", 1, CalculateAverage_kjetilvcm::parseValue)
        ).read(splitter, columns ->
            m.compute(
                (LineSegment) columns.get("station"),
                (_, existing) ->
                    (existing == null ? new Result() : existing)
                        .merge(columns.getInt("measurement"))
            ));
        return m;
    }

    private static int parseValue(LineSegment segment) {
        int value = 0;
        int pos = 1;
        long head = segment.head();
        long len = segment.length();
        for (long i = len - 1; i >= 0; i--) {
            long shift = i * 8;
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

        private int min = Integer.MAX_VALUE;

        private int max = Integer.MIN_VALUE;

        private int sum;

        public Result() {
        }

        Result(int count, int min, int max, int sum) {
            this.count = count;
            this.min = min;
            this.max = max;
            this.sum = sum;
        }

        Result(int value) {
            this.count = 1;
            this.min = value;
            this.max = value;
            this.sum = value;
        }

        public String toString() {
            return round(min) + "/" + round(1.0 * sum / count) + "/" + round(max);
        }

        public Result become(int count, int min, int max, int sum) {
            this.count = count;
            this.min = min;
            this.max = max;
            this.sum = sum;
            return this;
        }

        public Result freeze() {
            return new Result(count, min, max, sum);
        }

        public Result merge(Result in) {
            if (in == null) {
                return this;
            }
            min = Math.min(min, in.min);
            max = Math.max(max, in.max);
            sum += in.sum;
            count += in.count;
            return this;
        }

        Result merge(int value) {
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

    private static class LineSegmentBytesReader implements BytesReader<LineSegment> {

        @Override
        public LineSegment read(Bytes in, LineSegment using) {
            ReusableLineSegment segment = using instanceof ReusableLineSegment reusableLineSegment
                ? reusableLineSegment
                : new ReusableLineSegment(128);
            int length = in.readInt();
            int count = in.readInt();
            segment.setLength(length);
            for (int i = 0; i < count; i++) {
                long data = in.readLong();
                segment.setLong(i, data);
            }
            return segment;
        }
    }

    private static class LineSegmentBytesWriter implements BytesWriter<LineSegment> {

        @Override
        public void write(Bytes out, LineSegment segment) {
            long length = segment.length();
            long count = segment.shiftedLongsCount();
            out.writeInt(Math.toIntExact(length));
            out.writeInt(Math.toIntExact(count));
            segment.longStream(true)
                .forEach(data ->
                    out.writeLong(data));
        }
    }

    private static class ResultBytesReader implements BytesReader<Result> {

        @Override
        public Result read(Bytes in, Result using) {
            return (using == null ? new Result() : using).become(
                in.readInt(),
                in.readInt(),
                in.readInt(),
                in.readInt()
            );
        }
    }

    private static class ResultBytesWriter implements BytesWriter<Result> {

        @Override
        public void write(Bytes out, Result toWrite) {
            out.writeInt(toWrite.count);
            out.writeInt(toWrite.min);
            out.writeInt(toWrite.max);
            out.writeInt(toWrite.sum);
        }
    }
}
