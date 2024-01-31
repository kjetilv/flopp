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
package onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

public final class CalculateAverage_kjetilv {

    public static void main(String[] args) throws IOException {
        runJava();
    }

    public static Stream<LineSegment> lines(Partition partition, MemorySegmentSource memorySegmentSource) {
        return StreamSupport.stream(
            new BitwisePartitionSpliterator(
                partition,
                memorySegmentSource
            ),
            false
        );
    }

    private CalculateAverage_kjetilv() {
    }

    private static final String FILE = "./measurements.txt";

    private static void runJava() throws IOException {
        Instant start = Instant.now();
        Path path = Path.of(FILE);
        ForkJoinPool executor = new ForkJoinPool(Runtime.getRuntime().availableProcessors() * 2);
        long size = Files.size(path);
        int partitionCount = Runtime.getRuntime().availableProcessors() * 2;
        List<Partition> partitions = new Partitioning(partitionCount, 128)
            .of(size);
        List<CompletableFuture<Map<String, Result>>> list = partitions.stream()
            .map(partition ->
                CompletableFuture.supplyAsync(
                    () ->
                        lines(partition, new MemorySegmentSource(path, Arena::global, size, 128)),
                    executor
                ))
                    .map(future ->
                future.thenApply(CalculateAverage_kjetilv::toMap))
                    .toList();

        List<Map<String, Result>> maps = list.stream()
            .map(CompletableFuture::join)
            .toList();
        System.out.println(Duration.between(start, Instant.now()));
        Set<String> keys = keySet(maps);
        Map<String, Result> map = combineMaps(keys, maps);
        System.out.println(map);
        System.out.println(Duration.between(start, Instant.now()));
    }

    private static Set<String> keySet(List<Map<String, Result>> maps) {
        return maps.stream()
            .map(Map::keySet).flatMap(Collection::stream)
            .collect(Collectors.toSet());
    }

    private static Map<String, Result> toMap(Stream<LineSegment> lines) {
        try (lines) {
            Map<String, Result> m = new HashMap<>(1024, 1.0f);
            lines.forEach(ls -> {
                long length = ls.length(); //segment.length();
                int splitIndex = semiIndex(ls, length);
                int value = parseValue(
                    Math.toIntExact(length),
                    splitIndex,
                    ls
                );
                String key = ls.asString(splitIndex);
                m.compute(
                    key,
                    (_, existing) ->
                        existing == null
                            ? new Result(value)
                            : existing.collect(value)
                );
            });
            return m;
        }
    }

    private static Map<String, Result> combineMaps(
        Set<String> keys,
        List<Map<String, Result>> maps
    ) {
        TreeMap<String, Result> treeMap = new TreeMap<>(maps.getFirst());
        maps.stream().skip(1)
            .forEach(map ->
                keys.forEach(key -> {
                    Result base = treeMap.get(key);
                    Result addendum = map.get(key);
                    if (base == null) {
                        treeMap.put(key, addendum);
                    } else if (addendum != null) {
                        Result merged = base.merge(addendum);
                        treeMap.put(key, merged);
                    }
                }));
        return treeMap;
    }

    private static int semiIndex(LineSegment ls, long length) {
        for (int i = Math.toIntExact(length - 3); i >= 0; i--) {
            if (ls.byteAt(i) == ';') {
                return i;
            }
        }
        throw new IllegalStateException("No split");
    }

    private static int parseValue(int length, int splitIndex, LineSegment ls) {
        int value = 0;
        int boundary = splitIndex + 1;
        int pos = 1;
        for (int i = length - 1; i >= boundary; i--) {
            byte b = ls.byteAt(i);
            if (b == '.') {
                continue;
            }
            if (b == '-') {
                return value * -1;
            }
            value += (b - '0') * pos;
            pos *= 10;
        }
        return value;
    }

    public static final class LineSegments {

        public static String toString(LineSegment line) {
            return toString(line, Math.toIntExact(line.length()));
        }

        public static String toString(LineSegment line, int len) {
            byte[] bytes = new byte[len];
            for (int i = 0; i < len; i++) {
                bytes[i] = line.byteAt(i);
            }
            return new String(bytes);
        }

        private LineSegments() {
        }
    }

    @SuppressWarnings("unused")
    public record Partitioning(
        int partitionCount,
        long shortTail
    ) {

        public int partitionCount(boolean tailed) {
            return partitionCount + (tailed && shortTail > 0 ? 1 : 0);
        }

        public List<Partition> of(long total) {
            return partitions(partitionSizes(total));
        }

        private long[] partitionSizes(long total) {
            long overshoot = total % 8;
            long alignedSlices = total / 8;
            long[] sizes = defaultDistributedAlignmentScaled(alignedSlices);
            if (overshoot != 0) {
                sizes[sizes.length - 1] += overshoot;
            }
            return sizes;
        }

        private long[] defaultDistributedAlignmentScaled(long alignedSlices) {
            long[] sizes = defaultDistributed(alignedSlices);
            for (int i = 0; i < sizes.length; i++) {
                sizes[i] *= 8;
            }
            return sizes;
        }

        private long[] defaultDistributed(long total) {
            long remainders = intSized(total % partitionCount);
            long baseCount = intSized(total / partitionCount);
            long[] sizes = new long[Math.toIntExact(partitionCount)];
            Arrays.fill(sizes, baseCount);
            for (int i = 0; i < remainders; i++) {
                sizes[i] += 1;
            }
            return sizes;
        }

        private static List<Partition> partitions(long[] sizes) {
            long offset = 0;
            List<Partition> partitions = new ArrayList<>(sizes.length);
            for (int i = 0; i < sizes.length; i++) {
                partitions.add(
                    new Partition(i, sizes.length, offset, sizes[i])
                );
                offset += sizes[i];
            }
            return partitions;
        }

        private static List<Partition> singlePartition(long total) {
            return List.of(new Partition(0, 1, 0, total));
        }

        private static int cpus() {
            return Runtime.getRuntime().availableProcessors();
        }

        private static int intSized(long count) {
            if (count > MAX_VALUE) {
                throw new IllegalStateException(STR."Expected integer-sized partition: \{count} > \{MAX_VALUE}");
            }
            return Math.toIntExact(count);
        }
    }

    public record Partition(int partitionNo, int partitionCount, long offset, long count)
        implements Comparable<Partition> {

        public Partition(int partitionNo, int partitionCount, long offset, long count) {
            this.partitionNo = partitionNo;
            this.partitionCount = partitionCount;
            this.offset = offset;
            this.count = count;
            if (partitionNo >= partitionCount) {
                throw new IllegalStateException(
                    STR."partitionNo >= partitionCount: \{partitionNo} >= \{partitionCount}"
                );
            }
        }

        public long length(long size, int longeestLine) {
            return Math.min(
                size - offset,
                bufferedTo(longeestLine + 1)
            );
        }

        @Override
        public int compareTo(Partition o) {
            return Integer.compare(partitionNo, o.partitionNo());
        }

        public boolean first() {
            return partitionNo == 0;
        }

        public boolean last() {
            return partitionNo == partitionCount - 1;
        }

        @Override
        public String toString() {
            String pos = first() ? "<"
                : last() ? ">"
                    : "";
            String frac = first() || last() ? "" : STR."/\{partitionCount}";
            return STR."\{getClass().getSimpleName()}[\{pos}\{partitionNo + 1}\{frac}@\{offset}+\{count}]";
        }

        public long bufferedTo(int size) {
            long simpleBuffer = count + size;
            if (simpleBuffer % 8 == 0) {
                return simpleBuffer;
            }
            return (simpleBuffer / 8 + 1) * 8;
        }
    }

    public static final class MemorySegmentSource {

        private final Path path;

        private final Supplier<Arena> arena;

        private final FileChannel channel;

        private final long size;

        private final int longestLine;

        private MemorySegmentSource(Path path, Supplier<Arena> arena, long size, int longestLine) {
            this.path = Objects.requireNonNull(path, "path");
            this.arena = Objects.requireNonNull(arena, "arena");

            this.size = size;
            this.longestLine = longestLine;

            RandomAccessFile result;
            try {
                result = new RandomAccessFile(path.toFile(), "r");
            } catch (Exception e) {
                throw new IllegalStateException(STR."\{this} could not access file", e);
            }
            RandomAccessFile randomAccessFile = result;
            this.channel = randomAccessFile.getChannel();
        }

        public MemorySegment open(Partition partition) {
            try {
                return channel.map(
                    READ_ONLY,
                    partition.offset(),
                    partition.length(this.size, longestLine),
                    arena.get()
                );
            } catch (IOException e) {
                throw new IllegalStateException(
                    STR."\{this} could not open [\{partition.offset()}-\{partition.length(
                        size,
                        longestLine
                    )}] for \{partition}",
                    e
                );
            }
        }

        @Override
        public String toString() {
            return STR."\{getClass().getSimpleName()}[\{path}]";
        }
    }

    @SuppressWarnings("PackageVisibleField")
    static final class MutableLine implements LineSegment {

        MemorySegment memorySegment;

        long offset;

        long length;

        @Override
        public MemorySegment memorySegment() {
            return memorySegment;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public String toString() {
            return STR."\{getClass().getSimpleName()}[\{offset()}+\{length()}]";
        }
    }

    private static final class Result {

        private int count;

        private int min;

        private int max;

        private int sum;

        private Result(int value) {
            this.min = value;
            this.max = value;
            this.sum = value;
            this.count = 1;
        }

        public String toString() {
            return STR."\{round(min)}/\{round(1.0 * sum / count)}/\{round(max)}";
        }

        public Result merge(Result coll) {
            min = Math.min(min, coll.min);
            max = Math.max(max, coll.max);
            sum += coll.sum;
            count += coll.count;
            return this;
        }

        Result collect(int value) {
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

    private static final class BitwisePartitionSpliterator extends Spliterators.AbstractSpliterator<LineSegment> {

        private final Partition partition;

        private final MemorySegmentSource memorySegmentSource;

        private final MutableLine line;

        /**
         * Current mask
         */
        private long mask;

        /**
         * Current offset into partition, aligned to long size (8 bytes)
         */
        private long alignedOffset;

        /**
         * Current offset into partition, by byte
         */
        private long offset;

        /**
         * Position of line in progress
         */
        private long lineStart;

        private BitwisePartitionSpliterator(
            Partition partition,
            MemorySegmentSource memorySegmentSource
        ) {
            super(Long.MAX_VALUE, IMMUTABLE | SIZED);
            this.partition = Objects.requireNonNull(partition, "partition");
            this.memorySegmentSource = Objects.requireNonNull(memorySegmentSource, "memorySegmentSource");

            this.alignedOffset = -8;
            this.line = new MutableLine();
        }

        @Override
        public boolean tryAdvance(Consumer<? super LineSegment> action) {
            try {
                line.memorySegment = memorySegmentSource.open(partition);
                if (!partition.first()) {
                    skipToStart();
                }
                long limit = partition.count();
                if (!partition.last()) {
                    processAligned(action, limit);
                } else {
                    processTail(action, limit);
                }
                return false;
            } catch (Exception e) {
                throw new IllegalStateException(STR."\{this} failed: \{action}", e);
            }
        }

        @Override
        public String toString() {
            return STR."\{getClass().getSimpleName()}[offset:\{offset} \{partition}]";
        }

        private void skipToStart() {
            do {
                loadLong();
            } while (mask == 0);
            progressMask();
            clear();
            lineStart = offset;
        }

        private void processAligned(Consumer<? super LineSegment> action, long limit) {
            while (offset <= limit) {
                while (mask == 0) {
//                { // loadLong unrolled
                    offset = alignedOffset += 8;
                    long l = line.memorySegment.get(JAVA_LONG, alignedOffset);
                    mask = mask(l);
//                }
                }
                while (mask != 0) { // feedLines unrolled
//                { // progressMask unrolled
                    offset += Long.numberOfTrailingZeros(mask) / 8;
//                }
//                { // feedLine runrolled
                    long shift = offset - lineStart;

                    line.offset = lineStart;
                    line.length = shift;
                    lineStart += shift + 1;

                    action.accept(line);
//                }
//                { // clear unrolled
                    offset++;
                    mask &= CLEARED[Math.toIntExact(offset - alignedOffset)];
//                }
                }
            }
        }

        private void processTail(Consumer<? super LineSegment> action, long limit) {
            long tail = partition.last()
                ? partition.count() % 8
                : 0L;
            long lastLoadableOffset = limit - tail - 8;
            while (offset < limit) {
                while (mask == 0 && offset < lastLoadableOffset) {
                    loadLong();
                }
                if (mask == 0 && tail > 0) {
                    loadTail(tail);
                }
                feedLines(action);
            }
        }

        private void feedLines(Consumer<? super LineSegment> action) {
            while (mask != 0) {
                progressMask();
                feedLine(action);
                clear();
            }
        }

        private void feedLine(Consumer<? super LineSegment> action) {
            long shift = offset - lineStart;

            line.offset = lineStart;
            line.length = shift;
            lineStart += shift + 1;

            action.accept(line);
        }

        private void loadLong() {
            offset = alignedOffset += 8;
            long l = line.memorySegment.get(JAVA_LONG, alignedOffset);
            mask = mask(l);
        }

        private void loadTail(long tail) {
            offset = alignedOffset += 8;
            long l = loadBytes(tail);
            mask = mask(l);
        }

        private void progressMask() {
            offset += Long.numberOfTrailingZeros(mask) / 8;
        }

        private long loadBytes(long count) {
            long l = 0;
            for (long i = count - 1; i >= 0; i--) {
                byte b = line.memorySegment.get(JAVA_BYTE, alignedOffset + i);
                l = (l << 8) + b;
            }
            return l;
        }

        private void clear() {
            offset++;
            mask &= CLEARED[Math.toIntExact(offset - alignedOffset)];
        }

        private static final long[] CLEARED = {
            0xFFFFFFFFFFFFFFFFL,
            0xFFFFFFFFFFFFFF00L,
            0xFFFFFFFFFFFF0000L,
            0xFFFFFFFFFF000000L,
            0xFFFFFFFF00000000L,
            0xFFFFFF0000000000L,
            0xFFFF000000000000L,
            0xFF00000000000000L,
            0x0000000000000000L
        };

        private static long mask(long l) {
            long masked = l ^ 0x0A0A0A0A0A0A0A0AL;
            long underflown = masked - 0x0101010101010101L;
            long clearedHighBits = underflown & ~masked;
            return clearedHighBits & 0x8080808080808080L;
        }
    }

    public interface LineSegment {

        MemorySegment memorySegment();

        long offset();

        long length();

        default String asString(int length) {
            return LineSegments.toString(this, length);
        }

        default byte byteAt(int i) {
            return memorySegment().get(JAVA_BYTE, offset() + i);
        }
    }
}
