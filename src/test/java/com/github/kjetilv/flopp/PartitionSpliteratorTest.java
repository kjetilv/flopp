package com.github.kjetilv.flopp;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionSpliteratorTest {

    @SuppressWarnings("StatementWithEmptyBody")
    @Test
    void verySimpleTest() {
        byte[] bytes = """
            HEADER
            CONTNT
            FOOTER
            """.getBytes(StandardCharsets.US_ASCII);
        PartitionSpliterator spliterator = new PartitionSpliterator(
            new MyByteSource(bytes, 0),
            new Partition(0, 1, 0, bytes.length),
            Shape.size(bytes.length).header(1, 1)
                .longestLine(16)
                .charset(StandardCharsets.US_ASCII),
            50
        );
        List<NpLine> lines = new ArrayList<>();
        while (spliterator.tryAdvance(lines::add)) {
        }
        assertThat(lines).containsExactly(new NpLine("CONTNT", 0, 1));

    }

    @Test
    void simpleTest() {
        String str = "mississippiburningvhpictures";
        for (int size = 6; size < str.length(); size++) {
            for (int partitionCount = 2; partitionCount < size / 3; partitionCount++) {
                for (int bufferSize = 1; bufferSize < 10; bufferSize++) {
                    go(str, partitionCount, size, 10 * bufferSize);
                }
            }
        }
    }

    @Test
    void threeTwelveTest() {
        go("mississippiburningvhpictures", 3, 12, 10);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private static void go(String str, int partitionCount, int size, int bufferSize) {
        List<NpLine> lines = new ArrayList<>();
        try {
            String contents = Stream.of(
                    Stream.of("HEADER"),
                    IntStream.range(0, size)
                        .mapToObj(offset -> "%2d".formatted(offset) + str.substring(offset)),
                    Stream.of("FOOTER\n")
                )
                .flatMap(Function.identity())
                .collect(Collectors.joining("\n"));
            byte[] bytes = contents.getBytes(StandardCharsets.US_ASCII);
            List<Partition> partitions = Partition.partitions(bytes.length, partitionCount);
            for (Partition partition : partitions) {
                PartitionSpliterator spliterator0 = spliterator(bytes, partition, bufferSize);
                do {
                } while (spliterator0.tryAdvance(lines::add));
            }
            assertThat(lines)
                .describedAs(
                    state(partitionCount, size, lines, bufferSize))
                .hasSize(size);
            assertThat(
                "HEADER\n" +
                    lines.stream()
                        .map(NpLine::line)
                        .collect(Collectors.joining("\n")) +
                    "\nFOOTER\n")
                .describedAs(
                    state(partitionCount, size, lines, bufferSize)
                ).isEqualTo(contents);
            System.out.printf("%s partitions of %s lines, slice size %s%n", partitionCount, size, bufferSize);
        } catch (Exception e) {
            throw new IllegalStateException(state(partitionCount, size, lines, bufferSize), e);
        }
    }

    private static String state(int partitionCount, int size, List<NpLine> lines, int bufferSize) {
        return partitionCount + " partitions of " + size + " lines, slices " + bufferSize + ":\n" +
            lines.stream()
                .map(Object::toString)
                .collect(Collectors.joining("\n"));
    }

    private static PartitionSpliterator spliterator(byte[] bytes, Partition partition, int bufferSize) {
        MyByteSource bytesProvider = new MyByteSource(bytes, Math.toIntExact(partition.offset()));
        PartitionSpliterator spliterator = new PartitionSpliterator(
            bytesProvider,
            partition,
            Shape.size(bytes.length)
                .header(1, 1)
                .longestLine(10)
                .charset(StandardCharsets.US_ASCII),
            bufferSize
        );
        return spliterator;
    }

    private static final class MyByteSource implements ByteSource {

        private final byte[] source;

        private final int offset;

        private MyByteSource(byte[] source, int offset) {
            this.source = source;
            this.offset = offset;
        }

        @Override
        public int fill(byte[] bytes, int offset, int length) {
            if (this.offset + offset < source.length) {
                int toRead = Math.min(source.length - offset, length);
                System.arraycopy(source, this.offset + offset, bytes, 0, toRead);
                return toRead;
            }
            return -1;
        }
    }
}
