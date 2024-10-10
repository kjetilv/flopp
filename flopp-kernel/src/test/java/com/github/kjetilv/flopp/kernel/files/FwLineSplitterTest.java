package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.Partitioning;
import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.formats.Formats;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.Range;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("SameParameterValue")
class FwLineSplitterTest {

    @Test
    void test() {
        assertFileContents(
            "foobarzot",
            Formats.Fw.simple(Range.of(3, 6)),
            "bar"
        );
    }

    private static void assertFileContents(String contents, Format.FwFormat format, String... lines) {
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
        Consumer<LineSegment> splitter = LineSplitters.fwSink(
            format,
            line ->
                line.columns(UTF_8)
                    .forEach(splits::add)
        );

        try {
            try (Partitioned<Path> partitioned = PartitionedPaths.partitioned(path, Partitioning.single())) {
                partitioned.streamers()
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
