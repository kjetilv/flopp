package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.FwFormat;
import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.Range;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class BitwiseFwLineSplitterTest {

    @Test
    void test() {
        assertFileContents(
            "foobarzot",
            new FwFormat(new Range[] {Range.of(3, 6)}),
            "bar"
        );
    }

    private static void assertFileContents(String contents, FwFormat fwFormat, String... lines) {
        List<String> splits = new ArrayList<>();
        Path path = null;
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
            fwFormat,
            line ->
                line.columns()
                    .forEach(splits::add)
        );

        try {
            try (Partitioned<Path> partititioned = Bitwise.partititioned(path, Partitioning.single())) {
                partititioned.streams().streamers()
                    .forEach(streamer ->
                        streamer.lines()
                            .forEach(splitter));
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
                STR."""
                   Failed with list:
                     \{String.join("\n  ", splits)}
                   """, e);
        }
    }
}
