package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.files.FileChannelMemorySegmentSources;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Disabled
public class Vector2Test {

    @Test
    void test(@TempDir Path dir) throws IOException {
        Path file = getFile(
            dir,
            """
                abcdef
                0123456789
                xyz123klm234
                zot
                xyz123klm234fgh345
                xyz123klm234fgh345zotfipzfoo
                123
                abc
                """.split("\n")
        );

        try (
            MemorySegmentSources memorySegmentSources = new FileChannelMemorySegmentSources(file)
        ) {
            Partition partition = new Partition(1, 10, 4, 80);

            MemorySegmentPartitionSpliterator spliterator =
                new MemorySegmentPartitionSpliterator(
                    partition,
                    Shape.of(file),
                    () ->
                        memorySegment(partition, memorySegmentSources));

            spliterator.tryAdvance(line ->
                {
                    byte[] utf8String = line.memorySegment()
                        .asSlice(line.offset(), line.length())
                        .toArray(ValueLayout.JAVA_BYTE);
                    String x = new String(utf8String);
                    System.out.println(x);
                }
            );
        }
    }

    private static Path getFile(Path dir, String... lines) throws IOException {
        Stream<String> content = lines.length > 0 ? Arrays.stream(lines) : IntStream.range(5, 15)
            .mapToObj(i ->
                IntStream.range(5, 15).mapToObj(j -> {
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
            .flatMap(Function.identity());

        Path file = Files.writeString(
            dir.resolve(STR."\{UUID.randomUUID()}.bin"),
            content.collect(Collectors.joining("\n"))
        );

        System.out.println(new String(Files.readAllBytes(file)));
        return file;
    }

    private static MemorySegmentSource.Segment memorySegment(
        Partition partition,
        MemorySegmentSources memorySegmentSources
    ) {
        return Objects.requireNonNull(
            memorySegmentSources,
            "memorySegmentSources"
        ).source(partition).get();
    }
}
