package com.github.kjetilv.flopp.kernel;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
        assertThat(lines).containsExactly(new NpLine("CONTNT", 0, 2));
    }

    @Test
    void veryLongLineTest() {
        byte[] bytes = """
            HEADER
            abc
            defghidefghidefghidefghidefghidefghidefghi
            abc
            FOOTER
            """.getBytes(StandardCharsets.US_ASCII);
        List<NpLine> lines =
            drain(bytes, new Partition(0, 1, 0, bytes.length));

        assertThat(lines).containsExactly(
            new NpLine("abc", 0, 2),
            new NpLine("defghidefghidefghidefghidefghidefghidefghi", 0, 3),
            new NpLine("abc", 0, 4)
        );
    }

    @Test
    void veryLongLineTestMultiPartitions() {
        byte[] bytes = """
            HEADER
            abc
            def
            defghidefghidefghidefghidefghidefghidefghi
            abc
            FOOTER
            """.getBytes(StandardCharsets.US_ASCII);

        List<Partition> partitions = Partition.partitions(bytes.length, 2);

        List<NpLine> lines0 = drain(bytes, partitions.get(0));
        List<NpLine> lines1 = drain(bytes, partitions.get(1));

        assertThat(lines0).containsExactly(
            new NpLine("abc", 0, 2),
            new NpLine("def", 0, 3),
            new NpLine("defghidefghidefghidefghidefghidefghidefghi", 0, 4)
        );
        assertThat(lines1).containsExactly(
            new NpLine("abc", 1, 1));
    }

    @Test
    void veryLongLineTestEmptyPartitions() {
        byte[] bytes = """
            HEADER
            abc
            defghidefghidefghidefghidefghidefghidefghi
            abc
            FOOTER
            """.getBytes(StandardCharsets.US_ASCII);

        List<Partition> partitions = Partition.partitions(bytes.length, 3);

        List<NpLine> lines0 = drain(bytes, partitions.get(0));
        List<NpLine> lines1 = drain(bytes, partitions.get(1));
        List<NpLine> lines2 = drain(bytes, partitions.get(2));

        assertThat(lines0).containsExactly(
            new NpLine("abc", 0, 2),
            new NpLine("defghidefghidefghidefghidefghidefghidefghi", 0, 3)
        );
        assertThat(lines1).isEmpty();
        assertThat(lines2).containsExactly(
            new NpLine("abc", 2, 1));
    }

    @Test
    void veryLongLineTestEmptyPartitionsTrick() {
        byte[] bytes = """
            HEADER
             9piburningvhpicturesthisisthenextbigthing
            10iburningvhpicturesthisisthenextbigthing
             0mississippiburningvhpicturesthisisthenextbigthing
             6sippiburningvhpicturesthisisthenextbigthing
             5ssippiburningvhpicturesthisisthenextbigthing
             8ppiburningvhpicturesthisisthenextbigthing
            23turesthisisthenextbigthing
            20picturesthisisthenextbigthing
            19hpicturesthisisthenextbigthing
             3sissippiburningvhpicturesthisisthenextbigthing
             4issippiburningvhpicturesthisisthenextbigthing
            18vhpicturesthisisthenextbigthing
            15ingvhpicturesthisisthenextbigthing
            12urningvhpicturesthisisthenextbigthing
            13rningvhpicturesthisisthenextbigthing
            16ngvhpicturesthisisthenextbigthing
             2ssissippiburningvhpicturesthisisthenextbigthing
             1ississippiburningvhpicturesthisisthenextbigthing
            17gvhpicturesthisisthenextbigthing
            11burningvhpicturesthisisthenextbigthing
            22cturesthisisthenextbigthing
            21icturesthisisthenextbigthing
            14ningvhpicturesthisisthenextbigthing
             7ippiburningvhpicturesthisisthenextbigthing
            FOOTER
            """.getBytes(StandardCharsets.US_ASCII);

        List<Partition> partitions = Partition.partitions(bytes.length, 6);

        List<NpLine> lines0 = drain(bytes, partitions.get(0));
        List<NpLine> lines1 = drain(bytes, partitions.get(1));
        List<NpLine> lines2 = drain(bytes, partitions.get(2));
        List<NpLine> lines3 = drain(bytes, partitions.get(3));
        List<NpLine> lines4 = drain(bytes, partitions.get(4));
        List<NpLine> lines5 = drain(bytes, partitions.get(5));

        assertThat(Stream.of(
            lines0,
            lines1,
            lines2,
            lines3,
            lines4,
            lines5
        )).allSatisfy(npLines ->
            assertThat(npLines).hasSize(4));
    }

    @Test
    void simpleTest() {
        String str = "mississippiburningvhpicturesthisisthenextbigthing";
        for (int size = 6; size < str.length(); size++) {
            for (int partitionCount = 2; partitionCount < size / 3; partitionCount++) {
                for (int bufferSize = 1; bufferSize < 10; bufferSize++) {
                    go(str, partitionCount, size, bufferSize);
                    go(str, partitionCount, size, 4 * bufferSize);
                    go(str, partitionCount, size, 10 * bufferSize);
                }
            }
        }
    }

    @Test
    void trickyTest() {
        String str = "mississippiburningvhpicturesthisisthenextbigthing";
        go(str, 2, 9, 1);
    }

    @Test
    void threeTwelveTest() {
        go("mississippiburningvhpictures", 3, 12, 10);
    }

    private static List<NpLine> drain(byte[] bytes, Partition partition) {
        return StreamSupport.stream(spliterator(bytes, partition, 16), false)
            .toList();
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private static void go(String str, int partitionCount, int size, int bufferSize) {
        List<List<NpLine>> subLines = IntStream.range(0, partitionCount)
            .<List<NpLine>>mapToObj(i -> new ArrayList<>())
            .toList();
        List<NpLine> lines = new ArrayList<>();
        try {
            List<String> list =
                IntStream.range(0, size)
                        .mapToObj(offset -> "%2d".formatted(offset) + str.substring(offset))
                    .collect(Collectors.toCollection(ArrayList::new));
            Collections.shuffle(list);
            List<String> contents = Stream.of(
                    Stream.of("HEADER"),
                    list.stream(),
                    Stream.of("FOOTER")
                ).flatMap(Function.identity())
                .toList();
            byte[] bytes = (String.join("\n", contents) + "\n")
                .getBytes(StandardCharsets.US_ASCII);
            List<Partition> partitions = Partition.partitions(bytes.length, partitionCount);
            for (Partition partition : partitions) {
                PartitionSpliterator spliterator0 = spliterator(bytes, partition, bufferSize);
                do {
                } while (spliterator0.tryAdvance(npLine -> {
                    subLines.get(partition.partitionNo()).add(npLine);
                    lines.add(npLine);
                }));
            }
            assertThat(
                Stream.of(
                        Stream.of("HEADER"),
                        lines.stream()
                            .map(NpLine::line),
                        Stream.of("FOOTER")
                    )
                    .flatMap(Function.identity())
                    .toList())
                .describedAs(
                    state(partitionCount, size, lines, subLines, bufferSize)
                ).isEqualTo(contents);
            assertThat(lines)
                .describedAs(
                    state(partitionCount, size, lines, subLines, bufferSize))
                .hasSize(size);
        } catch (Exception e) {
            throw new IllegalStateException(state(partitionCount, size, lines, subLines, bufferSize), e);
        }
    }

    private static String state(
        int partitionCount,
        int size,
        List<NpLine> lines,
        List<List<NpLine>> subLines,
        int bufferSize
    ) {
        return partitionCount + " partitions of " + size + " lines, buffer size " + bufferSize + ": " +
               lines.size() + " lines:\n" +
               subLines.stream()
                   .map(sub ->
                       sub.stream()
                           .map(Object::toString)
                           .collect(Collectors.joining("\n")))
                          .map(Object::toString)
                   .map(ls -> "###\n" + ls + "\n")
                   .map(Objects::toString)
                   .collect(Collectors.joining());
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
