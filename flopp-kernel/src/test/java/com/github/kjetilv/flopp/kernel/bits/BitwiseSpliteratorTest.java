package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.MemorySegmentSources;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.files.FileChannelMemorySegmentSources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BitwiseSpliteratorTest {

    @Test
    void test(@TempDir Path dir) throws IOException {
        Stream<String> content =
            Stream.concat(
                Stream.of("a", "bb", "c"),
                IntStream.range(1, 5)
                    .mapToObj(i ->
                        IntStream.range(1, 5).mapToObj(j -> {
                            String n = IntStream.range(j, j + i)
                                .map(x -> x % 10)
                                .mapToObj(Integer::toString)
                                .collect(Collectors.joining());
                            String l = IntStream.range(j, j + i)
                                .map(x -> x % 26)
                                .map(x -> (int) x + 'a')
                                .mapToObj(x -> (char) x)
                                .map(Object::toString)
                                .collect(Collectors.joining());
                            return STR."\{l}\{n}";
                        }))
                    .flatMap(Function.identity())
            );

        Path file = Files.writeString(
            dir.resolve(STR."\{UUID.randomUUID()}.bin"),
            content.collect(Collectors.joining("\n")) + "\n"
        );

        Partitioning partitioning = Partitioning.longAligned(3);
        List<Partition> partitions = partitioning.of(Shape.of(file).size());

        System.out.println(new String(Files.readAllBytes(file)));
        try (
            MemorySegmentSources memorySegmentSources = new FileChannelMemorySegmentSources(
                file,
                partitioning.alignment()
            )
        ) {
            for (Partition partition : partitions) {

                BitwiseAlignedPartitionSpliterator spliterator =
                    new BitwiseAlignedPartitionSpliterator(
                        partition,
                        memorySegmentSources.source(partition).get().memorySegment()
                    );

                spliterator.tryAdvance(line ->
                    {
                        byte[] utf8String = line.memorySegment()
                            .asSlice(line.offset(), line.length())
                            .toArray(ValueLayout.JAVA_BYTE);
                        String x = new String(utf8String);
                        System.out.println(x);
                    }
                );
                System.out.println(STR."Done with \{partition}");
            }
        }
    }
}
