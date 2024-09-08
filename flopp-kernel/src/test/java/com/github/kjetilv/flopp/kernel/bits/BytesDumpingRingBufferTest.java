package com.github.kjetilv.flopp.kernel.bits;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BytesDumpingRingBufferTest {

    @Test
    void test() {
        List<String> strings = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();
        byte[] bytes = new byte[10];
        try (
            BytesDumpingRingBuffer buffer = new BytesDumpingRingBuffer(
                bytes,
                length -> {
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

    private static void line(BytesDumpingRingBuffer buffer, String foobar) {
        buffer.accept(foobar.getBytes(StandardCharsets.UTF_8), (byte)'\n');
    }

}
