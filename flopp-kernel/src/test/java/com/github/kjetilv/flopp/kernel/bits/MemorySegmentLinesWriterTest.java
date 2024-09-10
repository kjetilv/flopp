package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.io.LinesWriter;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.LineSegments;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class MemorySegmentLinesWriterTest {

    @Test
    void writeSinmple() throws IOException {
        Path tempFile = Files.createTempFile(UUID.randomUUID().toString(), ".txt");
        try (LinesWriter<LineSegment> writer = new MemorySegmentLinesWriter(tempFile, 100)) {
            writer.accept(LineSegments.of("foobar"));
        }
        assertThat(tempFile).hasContent("foobar");
    }

    @Test
    void writeCyling() throws IOException {
        Path tempFile = Files.createTempFile(UUID.randomUUID().toString(), ".txt");
        try (LinesWriter<LineSegment> writer = new MemorySegmentLinesWriter(tempFile, 8)) {
            writer.accept(LineSegments.of("foobar\n"));
            writer.accept(LineSegments.of("zotzip\n"));
        }
        assertThat(tempFile).hasContent(
            """
                foobar
                zotzip
                """);
    }

}