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
import com.github.kjetilv.flopp.kernel.formats.Formats;
import com.github.kjetilv.flopp.kernel.readers.Column;
import com.github.kjetilv.flopp.kernel.readers.ColumnReader;
import com.github.kjetilv.flopp.kernel.readers.ColumnReaders;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class FormattSplit_kjetilvlong {

    public static void main(String[] args) {
        for (String arg : args) {
            Instant start = Instant.now();
            Path path = Path.of(arg);
            Shape shape = Shape.of(path, UTF_8).longestLine(128);

            Partitioning partitioning = Partitioning.create(
                Runtime.getRuntime().availableProcessors(),
                shape.longestLine()
            ).scaled(2);

            LongAdder longAdder = add(partitioning, shape, path);
            System.out.println(longAdder);
            System.out.println(Duration.between(start, Instant.now()));
        }
    }

    public static LongAdder add(Partitioning partitioning, Shape shape, Path path) {
        LongAdder longAdder = new LongAdder();
        int chunks = partitioning.of(shape.size()).size();
        ColumnReader columnReader = ColumnReaders.create(
            Column.ofString("station", 1),
            Column.ofType("temperature", 2, FormattSplit_kjetilvlong::parseValue)
        );
        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path, partitioning, shape);
            ExecutorService executor = new ThreadPoolExecutor(
                chunks,
                chunks,
                0, TimeUnit.NANOSECONDS,
                new LinkedBlockingQueue<>(chunks)
            )
        ) {
            bitwisePartitioned.splitters(Formats.Csv.escape())
                .map(countFuture(columnReader, longAdder, executor))
                .toList()
                .forEach(CompletableFuture::join);
        }
        return longAdder;
    }

    private static int parseValue(LineSegment segment) {
        long value = 0;
        long pos = 1;
        long head = segment.head();
        int intExact = Math.toIntExact(segment.length());
        for (int i = intExact - 1; i >= 0; i--) {
            int shift = i * 8;
            long b = head >> shift & 0xFF;
            if (b == '.') {
                continue;
            }
            if (b == '-') {
                return (int)value * -1;
            }
            long j = b - '0';
            value += j * pos;
            pos *= 10;
        }
        return (int)value;
    }


    private FormattSplit_kjetilvlong() {
    }

    private static Function<PartitionedSplitter, CompletableFuture<Void>> countFuture(
        ColumnReader columnReader,
        LongAdder longAdder,
        ExecutorService executor
    ) {
        return splitsConsumer ->
            CompletableFuture.runAsync(
                () ->
                    columnReader.read(splitsConsumer, _ ->
                        longAdder.increment()),
                executor
            );
    }
}
