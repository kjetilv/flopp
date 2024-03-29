package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("StringTemplateMigration")
class LineSegmentTest {

    @TempDir
    private Path dir;

    @Test
    void testAligned() {
        LineSegment ls = LineSegments.of(
            """
                foo bar zot
                foo bar zot
                foo bar zot
                """);
        assertThat(ls.length()).isEqualTo(36L);
        assertThat(ls.isAlignedAtEnd()).isFalse();
        assertThat(ls.longEnd()).isEqualTo(32L);
        assertThat(ls.longCount()).isEqualTo(4L);
        assertThat(ls.fullLongCount()).isEqualTo(4L);
        assertThat(ls.startIndex()).isEqualTo(0L);
        assertThat(Bits.toString(ls.longNo(0))).isEqualTo("foo bar ");
        assertThat(Bits.toString(ls.fullLongNo(0))).isEqualTo("foo bar ");
        assertThat(Bits.toString(ls.tail(), ls.tailLength())).isEqualTo("zot\n");

        LineSegment slice = ls.slice(5L, 24L);

        assertThat(slice.length()).isEqualTo(19L);
        assertThat(slice.longEnd()).isEqualTo(24L);
        assertThat(slice.longCount()).isEqualTo(3L);
        assertThat(slice.fullLongCount()).isEqualTo(2L);
        assertThat(slice.startIndex()).isEqualTo(5L);
        assertThat(Bits.toString(slice.fullLongNo(0))).isEqualTo("zot\nfoo ");

        assertThat(Bits.toString(slice.head(), slice.headLength())).isEqualTo(slice.asString().substring(0, 3));
    }

    @Test
    void readPart() throws IOException {
        String line = IntStream.range(0, 10).mapToObj(
                _ -> IntStream.range(0, 10).mapToObj(String::valueOf)
                    .collect(Collectors.joining()))
            .collect(Collectors.joining());
        byte[] contents = (line + '\n').getBytes();
        Path path = Files.write(
            dir.resolve(Path.of(UUID.randomUUID() + ".txt")),
            contents
        );

        try (
            RandomAccessFile r = new RandomAccessFile(path.toFile(), "r");
            FileChannel channel = r.getChannel()
        ) {
            MemorySegment memorySegment = channel.map(READ_ONLY, 0, contents.length, Arena.ofAuto());
            LineSegment lineSegment = LineSegments.of(memorySegment, 13, 49);

            assertThat(lineSegment.longStart()).isEqualTo(8);
            assertThat(lineSegment.fullLongStart()).isEqualTo(16);
            assertThat(lineSegment.longEnd()).isEqualTo(48);
            assertThat(lineSegment.longCount()).isEqualTo(5);
            assertThat(lineSegment.fullLongCount()).isEqualTo(4);

            long firstAsBytes = lineSegment.bytesAt(0, 3);

            String bytedSubstring = Bits.toString(firstAsBytes, 3, UTF_8);
            String wantedSubstring = line.substring(13, 16);
            assertThat(bytedSubstring).isEqualTo(wantedSubstring);

            long first = lineSegment.head(lineSegment.headStart());
            assertThat(first).isEqualTo(firstAsBytes);

            assertThat(Bits.toString(first, 3, UTF_8)).isEqualTo(wantedSubstring);

//            for (int i = 0; i < lineSegment.longCount(); i++) {
//                long l = lineSegment.longNo(i);
//                System.out.println(toString(l, 8));
//            }
//
            long last = lineSegment.tail();
            String lastString = Bits.toString(last, 1, UTF_8);
            String wantedLastString = line.substring(48, 49);
            assertThat(lastString).isEqualTo(wantedLastString);
        }
    }
}
