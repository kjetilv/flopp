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
        runSpull();
    }

    private static final String FILE = "./measurements.txt";

    private static void runSpull() {
        Instant start = Instant.now();
        try (
            PartitionedPath partitionedPath = PartitionedPaths.create(
                Path.of(FILE),
                Partitioning.create(Runtime.getRuntime().availableProcessors() * 2, 65536),
                Executors.newVirtualThreadPerTaskExecutor()
            )
        ) {
            List<ByteArrayToResultMap> list =
                partitionedPath.mapSegmentPartition((partition, segs) -> {
//                    Map<String, Result> map = new HashMap<>();
                    ByteArrayToResultMap m = new ByteArrayToResultMap();
                    segs.forEach(seg -> {
                        int offset = seg.offset(); //segment.offset();
                        int length = seg.length(); //segment.length();
                        byte[] bytes = seg.bytes();
//                        byte[] bytes = segment.bytes();
                        int splitIndex = -1;
                        int hash = 1;
                        for (int i = offset; i < length; i++) {
                            if (bytes[i] == ';') {
                                splitIndex = i;
                                break;
                            } else {
                                hash = 31 * hash + 17 * bytes[i];
                            }
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
        try (
            PartitionedPath partitionedPath = PartitionedPaths.create(
                Path.of(FILE),
                Partitioning.create(Runtime.getRuntime().availableProcessors() * 2, 65536),
                Executors.newVirtualThreadPerTaskExecutor()
            )
        ) {
            List<Map<String, Result>> list =
                partitionedPath.mapSegmentPartition((partition, segs) -> {
//                    Map<String, Result> map = new HashMap<>();
                    Map<String, Result> m = new HashMap<>();
                    segs.forEach(seg -> {
                        int offset = seg.offset(); //segment.offset();
                        int length = seg.length(); //segment.length();
                        byte[] bytes = seg.bytes();
//                        byte[] bytes = segment.bytes();
                        int splitIndex = -1;
                        int hash = 1;
                        for (int i = offset; i < length; i++) {
                            if (bytes[i] == ';') {
                                splitIndex = i;
                                break;
                            } else {
                                hash = 31 * hash + 17 * bytes[i];
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
                            TreeMap::new)
                    );
            System.out.println(collect);
            System.out.println(Duration.between(start, Instant.now()));
        }
    }

    private static short parseValue(int length, int splitIndex, byte[] bytes) {
        short value = bytes[bytes.length - 1];
        short pos = 1;
        for (int i = length - 2; i >= splitIndex + 1; i--) {
            byte b = bytes[i];
            if (b == '.') {
                continue;
            }
            if (b == '-') {
                value *= -1;
                continue;
            }
            value += (short) (pos * (b - '0'));
            pos *= 10;
        }
        return value;
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
                if (!(keys[slot].length != size || diff(keys[slot], key, offset, size))) break;
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
                if (temp < value.min) {
                    value.min = temp;
                }
                if (temp > value.max) {
                    value.max = temp;
                }
                value.sum += temp;
                value.count += 1;
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

    private static boolean diff(byte[] key, byte[] key1, int offset, int size) {
        if (key[0] != key1[0]) {
            return true;
        }
        if (size > 0 && key[1] != key1[1]) {
            return true;
        }
        for (int i = 2; i < size; i++) {
            if (key[i] != key1[i]){
                return true;
            }
        }
        return false;
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
            return round(0.1d * min) + "/" + round(0.1d * sum / count) + "/" + round(0.1d * max);
        }

        public Result merge(Result coll) {
            this.min = coll.min < min ? coll.min : min;
            this.max = coll.max > max ? coll.max : max;
            this.sum += coll.sum;
            this.count += coll.count;
            return this;
        }

        Result collect(short v) {
            if (v < min) {
                min = v;
            }
            if (v > min) {
                max = v;
            }
            sum += v;
            count++;
            return this;
        }

        private static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
}
