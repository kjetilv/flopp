package com.github.kjetilv.lopp;

import com.github.kjetilv.lopp.utils.DumpingRingBuffer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DumpingRingBufferTest {

    @Test
    void test() throws IOException {
        List<String> strings = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();
        try (
            DumpingRingBuffer buffer = new DumpingRingBuffer(
                () -> new byte[10],
                (bytes, length) -> {
                    strings.add(new String(bytes, 0, length));
                    stringBuilder.append(new String(bytes, 0, length));
                }
            )
        ) {
            line(buffer, "foobar");
            line(buffer, "xxxyyyzzz");
            line(buffer, "zipzot");

            assertEquals(2, strings.size());
            assertEquals("foobar\nxxx", strings.get(0));
            assertEquals("yyyzzz\nzip", strings.get(1));
        }

        assertEquals(3, strings.size());
        assertEquals("zot\n", strings.get(2));

        assertEquals("""
                foobar
                xxxyyyzzz
                zipzot
                """,
                stringBuilder.toString());
    }

    private static void line(DumpingRingBuffer buffer, String foobar) {
        buffer.accept(foobar.getBytes(StandardCharsets.UTF_8), (byte)'\n');
    }

}
