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
import com.github.kjetilv.flopp.kernel.bits.LinesFormat;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

public final class JustSplit_kjetilvlong {

    public static void main(String[] args) {
        for (String arg : args) {
            Path path = Path.of(arg);
            Instant start = Instant.now();
            Shape shape = Shape.of(path).longestLine(128);

            Partitioning partitioning = Partitioning.create(
                Runtime.getRuntime().availableProcessors(),
                shape.longestLine()
            ).scaled(2);

            int chunks = partitioning.of(shape.size()).size();
            try (
                Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(path, partitioning, shape);
                ExecutorService executor = new ThreadPoolExecutor(
                    chunks,
                    chunks,
                    0, TimeUnit.NANOSECONDS,
                    new LinkedBlockingQueue<>(chunks)
                )
            ) {
                System.out.println(Duration.between(start, Instant.now()));
                LongAdder longAdder = new LongAdder();
                LinesFormat linesFormat = new LinesFormat(';', 2);
                List<CompletableFuture<Void>> list1 =
                    bitwisePartitioned.streams().lineSplitters(linesFormat)
                        .map(splitsConsumer ->
                            CompletableFuture.runAsync(
                                () ->
                                    splitsConsumer.accept(line ->
                                        longAdder.add(line.columnCount())),
                                executor
                            ))
                        .toList();
                list1.forEach(CompletableFuture::join);
                System.out.println(Duration.between(start, Instant.now()));
                System.out.println(longAdder);
                System.out.println(Duration.between(start, Instant.now()));
            }
        }
    }
}
