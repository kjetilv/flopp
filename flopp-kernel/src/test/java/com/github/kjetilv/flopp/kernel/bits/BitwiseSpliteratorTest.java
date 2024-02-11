package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Shape;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BitwiseSpliteratorTest {

    @Test
    void test(@TempDir Path dir) throws IOException {
        Stream<String> content =
            Stream.of(
                Stream.of("abcdefghijkl"),
                Stream.of("0", "", "a", "bb", "c"),
                IntStream.range(1, 5)
                    .mapToObj(i ->
                        IntStream.range(1, 5).mapToObj(j -> {
                            String n = IntStream.range(j, j + i)
                                .map(x -> x % 10)
                                .mapToObj(Integer::toString)
                                .collect(Collectors.joining());
                            String l = IntStream.range(j, j + i)
                                .map(x -> x % 26)
                                .map(x -> x + 'a')
                                .mapToObj(x -> (char) x)
                                .map(Object::toString)
                                .collect(Collectors.joining());
                            return STR."\{l}\{n}";
                        }))
                    .flatMap(Function.identity())
            ).flatMap(Function.identity());

        String csq = content.collect(Collectors.joining("\n")) + "\n";
        Path file = Files.writeString(
            dir.resolve(STR."\{UUID.randomUUID()}.bin"),
            csq
        );

        Shape shape = Shape.of(file).longestLine(10);
        Partitioning partitioning = Partitioning.create(3, 8);

        System.out.println(new String(Files.readAllBytes(file)));

        try (
            Partitioned<Path> bitwisePartitioned = Bitwise.partititioned(file, partitioning, shape);
        ) {
            bitwisePartitioned.streams().streamers().forEach(streamer -> {
                streamer.lines().forEach(line -> {
                    byte[] utf8String = line.memorySegment()
                        .asSlice(line.startIndex(), line.length())
                        .toArray(ValueLayout.JAVA_BYTE);
                    String x = "## " + new String(utf8String).replace("\n", "^\n");
                    System.out.println(x);
                });
                System.out.println(STR."Done with \{streamer.partition()}");
            });
//            for (Partition partition : partitions) {
//
//                BitwiseAlignedPartitionSpliterator spliterator =
//                    new BitwiseAlignedPartitionSpliterator(
//                        partition,
//                        memorySegmentSources.source(partition).get().memorySegment()
//                    );
//
//                spliterator.tryAdvance(line ->
//                    {
//                        byte[] utf8String = line.memorySegment()
//                            .asSlice(line.offset(), line.length())
//                            .toArray(ValueLayout.JAVA_BYTE);
//                        String x = new String(utf8String);
//                        System.out.println(x);
//                    }
//                );
//                System.out.println(STR."Done with \{partition}");
//            }
        }
    }
}
