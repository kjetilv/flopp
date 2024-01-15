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

import com.github.kjetilv.flopp.kernel.files.PartitionedPath;
import com.github.kjetilv.flopp.kernel.files.PartitionedPaths;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class CalculateAverage_kjetilv {

    public static void main(String[] args) {
//        runSpull();
        runJava();
    }

    private static final String FILE = "./measurements.txt";

    private static void runSpull() {
        Instant start = Instant.now();
        Path path = Path.of(FILE);
        try (
            PartitionedPath partitionedPath = PartitionedPaths.create(
                path,
                Shape.of(path).longestLine(64, true),
                Partitioning.create(Runtime.getRuntime().availableProcessors(), 8192),
                Executors.newVirtualThreadPerTaskExecutor()
            )
        ) {
            List<ByteArrayToResultMap> list =
                partitionedPath.mapMemorySegmentPartition((_, segs) -> {
//                    Map<String, Result> map = new HashMap<>();
                    ByteArrayToResultMap m = new ByteArrayToResultMap();
                    segs.forEach(line -> {
                        short offset = 0; //segment.offset();
                        short length = (short)line.length(); //segment.length();
                        byte[] bytes = line.asBytes();
                        int splitIndex = -1;
                        int hash = 1;
                        for (short i = offset; i < length; i++) {
                            if (bytes[i] == ';') {
                                splitIndex = i;
                                break;
                            } else {
                                hash = 31 * hash + bytes[i];
                            }
                        }
                        if (splitIndex == -1) {
                            throw new IllegalStateException(STR."Unparseable: \{new String(bytes)} in \{line}");
                        }
                        short value = parseValue(length, splitIndex, bytes);
                        m.putOrMerge(bytes, 0, splitIndex, value, hash);
                    });
                    return m;
                });
            TreeMap<String, Result> collect =
                list.stream().flatMap(byteArrayToResultMap -> byteArrayToResultMap.getAll()
                        .stream())
                    .collect(
                        Collectors.toMap(e -> new String(e.key()), Entry::value, Result::merge, TreeMap::new)
                    );
            System.out.println(collect);
            System.out.println(Duration.between(start, Instant.now()));
        }
    }

    private static void runJava() {
        Instant start = Instant.now();
        Path path = Path.of(FILE);
        try (
            PartitionedPath partitionedPath = PartitionedPaths.create(
                path,
                Shape.of(path).longestLine(64, true),
                Partitioning.create(Runtime.getRuntime().availableProcessors(), 65536),
                Executors.newVirtualThreadPerTaskExecutor()
            )
        ) {
            List<Map<String, Result>> list =
                partitionedPath.mapMemorySegmentPartition((partition, segs) -> {
//                    Map<String, Result> map = new HashMap<>();
                    Map<String, Result> m = new HashMap<>(1024, 1.0f);
                    segs.forEach(seg -> {
                        short length = (short) seg.length(); //segment.length();
                        byte[] bytes = MemorySegments.toBytes(seg);
                        int splitIndex = -1;
                        for (int i = length - 3;  i >= 0; i--) {
                            if (bytes[i] == ';') {
                                splitIndex = i;
                                break;
                            }
                        }
                        short value = parseValue(length, splitIndex, bytes);
                        m.compute(new String(bytes, 0, splitIndex), (s, result) ->
                            result == null ? new Result(value) : result.collect(value));
                    });
                    return m;
                });
            TreeMap<String, Result> collect =
                list.stream().flatMap(byteArrayToResultMap -> byteArrayToResultMap.entrySet()
                        .stream())
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            Result::merge,
                            TreeMap::new
                        )
                    );
            System.out.println(collect);
            System.out.println(Duration.between(start, Instant.now()));
        }
    }

    private static short parseValue(short length, int splitIndex, byte[] bytes) {
        short value = 0;
        int boundary = splitIndex + 1;
        int pos = 1;
        for (int i = length - 1; i >= boundary; i--) {
            byte b = bytes[i];
            if (b == '.') {
                continue;
            }
            if (b == '-') {
                value *= -1;
                continue;
            }
            int num = b - '0';
            value += (short) (num * pos);
            pos *= 10;
        }
        return value;
    }

    private static boolean diff(byte[] key, byte[] key1, int offset, int size) {
        if (key[0] != key1[0]) {
            return true;
        }
        if (size > 0 && key[1] != key1[1]) {
            return true;
        }
        for (int i = 2; i < size; i++) {
            if (key[i] != key1[i]) {
                return true;
            }
        }
        return false;
    }

    record Entry(byte[] key, Result value) {
    }

    static final class ByteArrayToResultMap {
        private final Result[] slots = new Result[MAPSIZE];

        private final byte[][] keys = new byte[MAPSIZE][];

        public void putOrMerge(byte[] key, int offset, int size, short temp, int hash) {
            int slot = hash & slots.length - 1;
            var slotValue = slots[slot];
            // Linear probe for open slot
            while (slotValue != null) {
                if (!(keys[slot].length != size || diff(keys[slot], key, offset, size))) {
                    break;
                }
                slot = (slot + 1) & (slots.length - 1);
                slotValue = slots[slot];
            }
            Result value = slotValue;
            if (value == null) {
                slots[slot] = new Result(temp);
                byte[] bytes = new byte[size];
                System.arraycopy(key, offset, bytes, 0, size);
                keys[slot] = bytes;
            } else {
                value.collect(temp);
            }
        }

        // Get all pairs
        public List<Entry> getAll() {
            List<Entry> result = new ArrayList<>(slots.length);
            for (int i = 0; i < slots.length; i++) {
                Result slotValue = slots[i];
                if (slotValue != null) {
                    result.add(new Entry(keys[i], slotValue));
                }
            }
            return result;
        }

        public static final int MAPSIZE = 1024 * 128;
    }

    private static final class Result {

        private int count;

        private short min;

        private short max;

        private int sum;

        private Result(short value) {
            this.min = value;
            this.max = value;
            this.sum += value;
        }

        public String toString() {
            return round(min) + "/" + round(1.0 * sum / count) + "/" + round(max);
        }

        public Result merge(Result coll) {
            if (coll.min < min) {
                min = coll.min;
            }
            if (coll.max > max) {
                max = coll.max;
            }
            this.sum += coll.sum;
            this.count += coll.count;
            return this;
        }

        Result collect(short v) {
            if (v < min) {
                min = v;
            }
            if (v > max) {
                max = v;
            }
            sum += v;
            count++;
            return this;
        }

        private static double round(double value) {
            return Math.round(value) / 10.0;
        }
    }
}
