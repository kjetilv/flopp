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

import com.github.kjetilv.flopp.kernel.bits.Bitwise;
import com.github.kjetilv.flopp.kernel.readers.Readers;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import static com.github.kjetilv.flopp.kernel.readers.Readers.column;
import static com.github.kjetilv.flopp.kernel.readers.Readers.reader;

public final class FormattSplit_kjetilvlong {

    public static void main(String[] args) {
        for (String arg : args) {
            Instant start = Instant.now();
            Path path = Path.of(arg);
            Shape shape = Shape.of(path).longestLine(128);

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
        Readers.Reader reader = reader(
            column("Station", 1),
            column("Temperature", 2, Double::parseDouble)
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
            CsvFormat csvFormat = new CsvFormat(';');
            bitwisePartitioned.csvSplitters().splitters(csvFormat)
                .map(countFuture(reader, longAdder, executor))
                .toList()
                .forEach(CompletableFuture::join);
        }
        return longAdder;
    }

    private static Function<PartitionedSplitter, CompletableFuture<Void>> countFuture(
        Readers.Reader reader,
        LongAdder longAdder,
        ExecutorService executor
    ) {
        return splitsConsumer ->
            CompletableFuture.runAsync(
                () ->
                    reader.read(splitsConsumer, entry ->
                        longAdder.increment()),
                executor
            );
    }
}
