package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.ByteSource;
import com.github.kjetilv.flopp.kernel.Partition;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class FileChannelByteSourceTest {

    @Test
    void shouldFill() throws IOException {
        String string = IntStream.range('a', 'z' + 1)
            .mapToObj(x -> (char) x)
            .map(Object::toString)
            .collect(Collectors.joining());
        Path tempFile = Files.createTempFile(UUID.randomUUID().toString(), ".txt");
        Files.write(tempFile, string.getBytes());
        try (
            RandomAccessFile randomAccessFile = randomAccess(tempFile);
            FileChannel channel = randomAccessFile.getChannel();
        ) {
            ByteSource source = new FileChannelByteSource(
                new Partition(0, 2, 0, 10),
                channel,
                Files.size(tempFile),
                4
            );
            byte[] filler = new byte[8];

            long filled = source.fill(filler);
            assertThat(filled).isEqualTo(8L);
            assertThat(new String(filler, 0, 8)).isEqualTo("abcdefgh");

            filled = source.fill(filler);
            assertThat(filled).isEqualTo(6L);
            assertThat(new String(filler, 0, 6)).isEqualTo("ijklmn");

            filled = source.fill(filler);
            assertThat(filled).isEqualTo(4L);
            assertThat(new String(filler, 0, 4)).isEqualTo("opqr");

            filled = source.fill(filler);
            assertThat(filled).isEqualTo(4L);
            assertThat(new String(filler, 0, 4)).isEqualTo("stuv");

            filled = source.fill(filler);
            assertThat(filled).isEqualTo(4L);
            assertThat(new String(filler, 0, 4)).isEqualTo("wxyz");

            filled = source.fill(filler);
            assertThat(filled).isEqualTo(-1);
        }
    }

    protected static RandomAccessFile randomAccess(Path path) {
        try {
            return new RandomAccessFile(path.toFile(), "r");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
