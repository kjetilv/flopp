package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.BitwisePartitionSpliterator;
import com.github.kjetilv.flopp.kernel.bits.MemorySegmentSource;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.Arena;
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

@Disabled
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
        Path path = getPath(dir, bytes);
        Shape shape = Shape.size(bytes.length).header(1, 1)
            .longestLine(16)
            .charset(StandardCharsets.US_ASCII);
        BitwisePartitionSpliterator spliterator = new BitwisePartitionSpliterator(
            partition,
            new MemorySegmentSource(path,
                shape,
                Arena.ofAuto()),
            PartitionMediator.create(partition, shape, LineSegment::immutable),
            null
        );
        List<String> lines = new ArrayList<>();
        while (spliterator.tryAdvance(e -> lines.add(e.asString()))) {
        }
        assertThat(lines).containsExactly("CONTNT");
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
        List<LineSegment> lines = drain(
            bytes,
            new Partition(0, 1, 0, bytes.length),
            2,
            dir
        );

        assertThat(lines).map(LineSegment::asString).containsExactly(
            "abc",
            "defghidefghidefghidefghidefghidefghidefghi",
            "abc"
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

        List<Partition> partitions = Partitioning.count(2).of(bytes.length);

        List<LineSegment> lines0 = drain(bytes, partitions.get(0), 2, dir);
        List<LineSegment> lines1 = drain(bytes, partitions.get(1), 2, dir);

        assertThat(lines0).map(LineSegment::asString).containsExactly(
            "abc",
            "def",
            "defghidefghidefghidefghidefghidefghidefghi"
        );
        assertThat(lines1).map(LineSegment::asString).containsExactly("abc");
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

        List<Partition> partitions = Partitioning.count(3).of(bytes.length);

        List<LineSegment> lines0 = drain(bytes, partitions.get(0), 10, dir);
        List<LineSegment> lines1 = drain(bytes, partitions.get(1), 10, dir);
        List<LineSegment> lines2 = drain(bytes, partitions.get(2), 10, dir);

        assertThat(lines0).map(LineSegment::asString).containsExactly(
            "abc",
            "defghidefghidefghidefghidefghidefghidefghi");
        assertThat(lines1).isEmpty();
        assertThat(lines2).map(LineSegment::asString).containsExactly("abc");
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

        List<Partition> partitions = Partitioning.count(6).of(bytes.length);

        List<LineSegment> lines0 = drain(bytes, partitions.get(0), 10, dir);
        List<LineSegment> lines1 = drain(bytes, partitions.get(1), 10, dir);
        List<LineSegment> lines2 = drain(bytes, partitions.get(2), 10, dir);
        List<LineSegment> lines3 = drain(bytes, partitions.get(3), 10, dir);
        List<LineSegment> lines4 = drain(bytes, partitions.get(4), 10, dir);
        List<LineSegment> lines5 = drain(bytes, partitions.get(5), 10, dir);

        assertThat(Stream.of(
            lines0,
            lines1,
            lines2,
            lines3,
            lines4,
            lines5
        ).mapToInt(List::size).sum()).isEqualTo(24);

        assertThat(lines3.get(4).asString()).isEqualTo(" 2ssissippiburningvhpicturesthisisthenextbigthing");
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

    private static Path getPath(Path dir, byte[] bytes) {
        try {
            return Files.write(dir.resolve(STR."\{UUID.randomUUID()}.bytes"), bytes);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<LineSegment> drain(byte[] bytes, Partition partition, int longestLine, Path dir) {
        return StreamSupport.stream(spliterator(
                bytes,
                partition,
                longestLine,
                dir
            ), false)
            .toList();
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private static void go(String str, int partitionCount, int size, int bufferSize, Path dir) {
        List<List<String>> subLines = IntStream.range(0, partitionCount)
            .<List<String>>mapToObj(_ -> new ArrayList<>())
            .toList();
        List<String> lines = new ArrayList<>();
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
            List<Partition> partitions = Partitioning.count(partitionCount).of(bytes.length);
            for (Partition partition : partitions) {
                BitwisePartitionSpliterator spliterator0 = spliterator(bytes, partition, 10, dir);
                do {
                } while (spliterator0.tryAdvance(nLine -> {
                    subLines.get(partition.partitionNo()).add(nLine.asString());
                    lines.add(nLine.asString());
                }));
            }
            assertThat(
                Stream.of(
                        Stream.of("HEADER"),
                        lines.stream(),
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
        List<String> lines,
        List<List<String>> subLines,
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

    private static BitwisePartitionSpliterator spliterator(
        byte[] bytes,
        Partition partition,
        int longestLine,
        Path dir
    ) {
        Path bytesProvider = getPath(dir, bytes);
        Shape shape = Shape.size(bytes.length)
            .header(1, 1)
            .longestLine(longestLine)
            .charset(StandardCharsets.US_ASCII);
        BitwisePartitionSpliterator spliterator = new BitwisePartitionSpliterator(
            partition,
            new MemorySegmentSource(bytesProvider,
                shape,
            Arena.ofAuto()),

            PartitionMediator.create(partition, shape, LineSegment::immutable),
            null);
        return spliterator;
    }
}
