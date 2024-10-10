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
import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.bits.PartitionedPaths;
import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.formats.Formats;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class JustSplit_kjetilvlong {
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
//        int chunks = partitioning.of(shape.size()).size();
        try (
            Partitioned<Path> bitwisePartitioned = PartitionedPaths.partitioned(path, partitioning, shape)
//            ExecutorService executor = new ThreadPoolExecutor(
//                chunks,
//                chunks,
//                0, TimeUnit.NANOSECONDS,
//                new LinkedBlockingQueue<>(chunks)
//            )
        ) {
            Format.Csv.Escape format = Formats.Csv.escape(';');
            List<Runnable> list1 =
                bitwisePartitioned.splitters(format)
                    .map(splitsConsumer ->
//                        CompletableFuture.runAsync(
                        (Runnable)
                            () ->
                                splitsConsumer.forEach(line ->
                                    longAdder.add(line.columnCount()))
//                        ,
//                            executor
                        )
//                    .map(cf -> (Runnable) cf::join)
                    .toList();
            list1.forEach(Runnable::run);
        }
        return longAdder;
    }

    private JustSplit_kjetilvlong() {

    }
}
