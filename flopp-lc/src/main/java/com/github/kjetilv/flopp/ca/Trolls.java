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
import com.github.kjetilv.flopp.kernel.readers.Reader;
import com.github.kjetilv.flopp.kernel.readers.Readers;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class Trolls {

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            for (String arg : args) {
                Instant start = Instant.now();
                Path path = Path.of(arg);
                Shape shape = Shape.of(path, UTF_8).header(1).longestLine(1024);

                Partitioning partitioning = Partitioning.create(
                    Runtime.getRuntime().availableProcessors(),
                    shape.longestLine()
                );

                LongAdder longAdder = add(partitioning, shape, path);
                System.out.println(longAdder);
                System.out.println(Duration.between(start, Instant.now()));
            }
        }
    }

    public static LongAdder add(Partitioning partitioning, Shape shape, Path path) {
        LongAdder longAdder = new LongAdder();
        int chunks = partitioning.of(shape.size()).size();
        CsvFormat format = CsvFormat.quoted(',', '"');
        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path, partitioning, shape);
            ExecutorService executor = new ThreadPoolExecutor(
                chunks,
                chunks,
                0, TimeUnit.NANOSECONDS,
                new LinkedBlockingQueue<>(chunks)
            )
        ) {
            LongAdder entryCount = new LongAdder();
            LongAdder failures = new LongAdder();
            LongAdder followersCount = new LongAdder();

            Consumer<Reader.Columns> entryHandler = map -> {
                entryCount.increment();
                Object followers = map.get("followers");
                long x1 = 0;
                try {
                    x1 = Long.parseLong(followers.toString());
                } catch (NumberFormatException e) {
                    failures.increment();
                    System.out.println(map);
                }
                followersCount.add(x1);
            };

            Function<PartitionedSplitter, CompletableFuture<Void>> partitionFuture = splitter ->
                    CompletableFuture.runAsync(
                        () ->
                            Readers.create(path, format).read(splitter, entryHandler),
                        executor
                    );
            PartitionedSplitters partitionedSplitters = bitwisePartitioned.splitters();
            partitionedSplitters.splitters(format)
                .map(partitionFuture)
                .toList()
                .forEach(CompletableFuture::join);

            System.out.println(followersCount.longValue() / entryCount.longValue());
            System.out.println(failures);
        }
        return longAdder;
    }

    private Trolls() {
    }
}
