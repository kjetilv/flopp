package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.partitions.Partitioning;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

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
                            return l + "/" + n;
                        }))
                    .flatMap(Function.identity())
            ).flatMap(Function.identity());

        String csq = content.collect(Collectors.joining("\n")) + "\n";
        Path file = Files.writeString(dir.resolve(UUID.randomUUID() + ".bin"), csq);

        Shape shape = Shape.of(file, UTF_8).longestLine(10);
        Partitioning partitioning = Partitioning.create(3, 8);

        List<String> expected;
        try (Stream<String> lines = Files.lines(file)) {
            expected = lines
                .map(line -> "## " + line.replace("\n", "^\n"))
                .toList();
        }

//        System.out.println(new String(Files.readAllBytes(file)));

        List<String> convs = new ArrayList<>();
        try (
            Partitioned<Path> bitwisePartitioned = PartitionedPaths.partitioned(file, partitioning, shape)
        ) {
            bitwisePartitioned.streamers()
                .forEach(streamer ->
                    streamer.lines()
                        .map(line -> {
                            byte[] utf8String = line.memorySegment()
                                .asSlice(line.startIndex(), line.length())
                                .toArray(ValueLayout.JAVA_BYTE);
                            return "## " + new String(utf8String).replace("\n", "^\n");
                        })
                        .forEach(convs::add));
        }

        assertThat(convs).containsExactlyElementsOf(expected);
    }
}
