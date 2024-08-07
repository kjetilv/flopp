package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class BitwiseFwLineSplitterTest {

    @Test
    void test() {
        assertFileContents(
            "foobarzot",
            new FwFormat(Range.of(3, 6)),
            "bar"
        );
    }

    private static void assertFileContents(String contents, FwFormat format, String... lines) {
        List<String> splits = new ArrayList<>();
        Path path;
        try {
            path = Files.write(
                Files.createTempFile(UUID.randomUUID().toString(), ".txt"),
                List.of(
                    contents
                        .split("\n")
                )
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        BitwiseFwLineSplitter splitter = new BitwiseFwLineSplitter(
            format,
            line ->
                line.columns(UTF_8)
                    .forEach(splits::add)
        );

        try {
            try (Partitioned<Path> partititioned = Bitwise.partititioned(path, Partitioning.single())) {
                partititioned.streams().streamers()
                    .forEach(streamer ->
                        streamer.lines().forEach(splitter));
            }
            if (lines.length > 0) {
                assertThat(splits).containsExactly(lines);
            } else {
                assertThat(splits).containsExactlyElementsOf(
                    Arrays.stream(contents.split("\n"))
                        .flatMap(s ->
                            Arrays.stream(s.split(";")))
                        .toList());
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed with list:\n  " + String.join("\n", splits), e);
        }
    }
}
