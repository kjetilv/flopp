package com.github.kjetilv.flopp.kernel.bits;

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
import static org.assertj.core.api.Assertions.assertThat;

class LineSegmentTest {

    @TempDir
    private Path dir;

    @Test
    void readPart() throws IOException {
        String line = IntStream.range(0, 10).mapToObj(
                i -> IntStream.range(0, 10).mapToObj(j -> STR."\{j}")
                    .collect(Collectors.joining()))
            .collect(Collectors.joining());
        byte[] contents = (line + '\n').getBytes();
        Path path = Files.write(
            dir.resolve(Path.of(STR."\{UUID.randomUUID().toString()}.txt")),
            contents
        );

        try (
            RandomAccessFile r = new RandomAccessFile(path.toFile(), "r");
            FileChannel channel = r.getChannel();
        ) {
            MemorySegment memorySegment = channel.map(READ_ONLY, 0, contents.length, Arena.ofAuto());
            LineSegment lineSegment = LineSegment.of(memorySegment, 13, 49);

            assertThat(lineSegment.longStart()).isEqualTo(8);
            assertThat(lineSegment.longEnd()).isEqualTo(48);
            assertThat(lineSegment.longCount()).isEqualTo(5);

            long firstAsBytes = lineSegment.bytesAt(0, 3);

            String bytedSubstring = toString(firstAsBytes, 3);
            String wantedSubstring = line.substring(13, 16);
            assertThat(bytedSubstring).isEqualTo(wantedSubstring);

            long first = lineSegment.getHeadLong();
            assertThat(first).isEqualTo(firstAsBytes);

            assertThat(toString(first, 3)).isEqualTo(wantedSubstring);

            for (int i = 0; i < lineSegment.longCount(); i++) {
                long l = lineSegment.longNo(i);
                System.out.println(toString(l, 8));
            }

            long last = lineSegment.getTail();
            String lastString = toString(last, 1);
            String wantedLastString = line.substring(48, 49);
            assertThat(lastString).isEqualTo(wantedLastString);
        }
    }

    private static String toString(long l, int len) {
        byte[] bytes = {
            (byte) (l & 0xFF),
            (byte) (l >> 8L & 0xFF),
            (byte) (l >> 16L & 0xFF),
            (byte) (l >> 24L & 0xFF),
            (byte) (l >> 32L & 0xFF),
            (byte) (l >> 40L & 0xFF),
            (byte) (l >> 48L & 0xFF),
            (byte) (l >> 56L & 0xFF)
        };
        return new String(bytes, 0, len);
    }
}
