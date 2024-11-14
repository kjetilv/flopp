package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.LineSegments;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class LineSegmentsWriterTest {

    @Test
    void writeSinmple() throws IOException {
        Path tempFile = Files.createTempFile(UUID.randomUUID().toString(), ".txt");
        try (LinesWriter<Stream<LineSegment>> writer = new LineSegmentsWriter(tempFile, 100)) {
            writer.accept(Stream.of(LineSegments.of("foobar")));
        }
        assertThat(tempFile).hasContent("foobar");
    }

    @Test
    void writeCyling() throws IOException {
        Path tempFile = Files.createTempFile(UUID.randomUUID().toString(), ".txt");
        try (LinesWriter<Stream<LineSegment>> writer = new LineSegmentsWriter(tempFile, 8)) {
            writer.accept(Stream.of(
                    "foobar\n",
                    "zotzip\n",
                    "coolx"
                )
                .map(LineSegments::of));
        }
        assertThat(tempFile).hasContent(
            """
                foobar
                zotzip
                coolx""");
    }

}