package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.files.FileChannelSources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

class NLinePartitionSpliteratorTest {

    @SuppressWarnings("StatementWithEmptyBody")
    @Test
    void verySimpleTest(@TempDir Path dir) {
        byte[] bytes = """
            HEADER
            CONTNT
            FOOTER
            """.getBytes(StandardCharsets.US_ASCII);
        Partition partition = new Partition(0, 1, 0, bytes.length);
        ByteSource byteSource = getByteSource(dir, bytes, partition);
        NLineGrowingPartitionSpliterator spliterator = new NLineGrowingPartitionSpliterator(
            byteSource,
            partition,
            Shape.size(bytes.length).header(1, 1)
                .longestLine(16)
                .charset(StandardCharsets.US_ASCII),
            50
        );
        List<NLine> lines = new ArrayList<>();
        while (spliterator.tryAdvance(lines::add)) {
        }
        assertThat(lines).containsExactly(new NLine("CONTNT", 0, 2));
    }

    @Test
    void veryLongLineTest(@TempDir Path dir) {
        byte[] bytes = """
            HEADER
            abc
            defghidefghidefghidefghidefghidefghidefghi
            abc
            FOOTER
            """.getBytes(StandardCharsets.US_ASCII);
        List<NLine> lines = drain(
            bytes,
            new Partition(0, 1, 0, bytes.length),
            2,
            8,
            dir
        );

        assertThat(lines).containsExactly(
            new NLine("abc", 0, 2),
            new NLine("defghidefghidefghidefghidefghidefghidefghi", 0, 3),
            new NLine("abc", 0, 4)
        );
    }

    @Test
    void veryLongLineTestMultiPartitions(@TempDir Path dir) {
        byte[] bytes = """
            HEADER
            abc
            def
            defghidefghidefghidefghidefghidefghidefghi
            abc
            FOOTER
            """.getBytes(StandardCharsets.US_ASCII);

        List<Partition> partitions = Partition.partitions(bytes.length, 2);

        List<NLine> lines0 = drain(bytes, partitions.get(0), 2, 8, dir);
        List<NLine> lines1 = drain(bytes, partitions.get(1), 2, 8, dir);

        assertThat(lines0).containsExactly(
            new NLine("abc", 0, 2),
            new NLine("def", 0, 3),
            new NLine("defghidefghidefghidefghidefghidefghidefghi", 0, 4)
        );
        assertThat(lines1).containsExactly(
            new NLine("abc", 1, 1));
    }

    @Test
    void veryLongLineTestEmptyPartitions(@TempDir Path dir) {
        byte[] bytes = """
            HEADER
            abc
            defghidefghidefghidefghidefghidefghidefghi
            abc
            FOOTER
            """.getBytes(StandardCharsets.US_ASCII);

        List<Partition> partitions = Partition.partitions(bytes.length, 3);

        List<NLine> lines0 = drain(bytes, partitions.get(0), 10, 16, dir);
        List<NLine> lines1 = drain(bytes, partitions.get(1), 10, 16, dir);
        List<NLine> lines2 = drain(bytes, partitions.get(2), 10, 16, dir);

        assertThat(lines0).containsExactly(
            new NLine("abc", 0, 2),
            new NLine("defghidefghidefghidefghidefghidefghidefghi", 0, 3)
        );
        assertThat(lines1).isEmpty();
        assertThat(lines2).containsExactly(
            new NLine("abc", 2, 1));
    }

    @Test
    void veryLongLineTestEmptyPartitionsTrick(@TempDir Path dir) {
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

        List<NLine> lines0 = drain(bytes, partitions.get(0), 10, 16, dir);
        List<NLine> lines1 = drain(bytes, partitions.get(1), 10, 16, dir);
        List<NLine> lines2 = drain(bytes, partitions.get(2), 10, 16, dir);
        List<NLine> lines3 = drain(bytes, partitions.get(3), 10, 16, dir);
        List<NLine> lines4 = drain(bytes, partitions.get(4), 10, 16, dir);
        List<NLine> lines5 = drain(bytes, partitions.get(5), 10, 16, dir);

        assertThat(Stream.of(
            lines0,
            lines1,
            lines2,
            lines3,
            lines4,
            lines5
        ).mapToInt(List::size).sum()).isEqualTo(24);

        assertThat(lines3.get(4).line()).isEqualTo(" 2ssissippiburningvhpicturesthisisthenextbigthing");
    }

    @Test
    void simpleTest(@TempDir Path dir) {
        String str = "mississippiburningvhpicturesthisisthenextbigthing";
        for (int size = 6; size < str.length(); size++) {
            for (int partitionCount = 2; partitionCount < size / 3; partitionCount++) {
                for (int bufferSize = 1; bufferSize < 10; bufferSize++) {
                    go(str, partitionCount, size, bufferSize, dir);
                    go(str, partitionCount, size, 4 * bufferSize, dir);
                    go(str, partitionCount, size, 10 * bufferSize, dir);
                }
            }
        }
    }

    @Test
    void trickyTest(@TempDir Path dir) {
        String str = "mississippiburningvhpicturesthisisthenextbigthing";
        go(str, 2, 9, 10, dir);
    }

    @Test
    void threeTwelveTest(@TempDir Path dir) {
        go("mississippiburningvhpictures", 3, 12, 10, dir);
    }

    @SuppressWarnings("resource")
    private static ByteSource getByteSource(Path dir, byte[] bytes, Partition partition) {
        try {
            Path path = Files.write(dir.resolve(UUID.randomUUID() + ".bytes"), bytes);
            return new FileChannelSources(path, Files.size(path)).source(partition);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<NLine> drain(byte[] bytes, Partition partition, int longestLine, int bufferSize, Path dir) {
        return StreamSupport.stream(spliterator(
                bytes,
                partition,
                longestLine,
                bufferSize,
                dir
            ), false)
            .toList();
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private static void go(String str, int partitionCount, int size, int bufferSize, Path dir) {
        List<List<NLine>> subLines = IntStream.range(0, partitionCount)
            .<List<NLine>>mapToObj(i -> new ArrayList<>())
            .toList();
        List<NLine> lines = new ArrayList<>();
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
                NLineGrowingPartitionSpliterator spliterator0 = spliterator(bytes, partition, 10, bufferSize, dir);
                do {
                } while (spliterator0.tryAdvance(nLine -> {
                    subLines.get(partition.partitionNo()).add(nLine);
                    lines.add(nLine);
                }));
            }
            assertThat(
                Stream.of(
                        Stream.of("HEADER"),
                        lines.stream()
                            .map(NLine::line),
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
        List<NLine> lines,
        List<List<NLine>> subLines,
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

    private static NLineGrowingPartitionSpliterator spliterator(
        byte[] bytes,
        Partition partition,
        int longestLine,
        int bufferSize,
        Path dir
    ) {
        ByteSource bytesProvider = getByteSource(dir, bytes, partition);
        NLineGrowingPartitionSpliterator spliterator = new NLineGrowingPartitionSpliterator(
            bytesProvider,
            partition,
            Shape.size(bytes.length)
                .header(1, 1)
                .longestLine(longestLine)
                .charset(StandardCharsets.US_ASCII),
            bufferSize
        );
        return spliterator;
    }
}
