package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class MemorySegments {

    public static MemorySegment of(String string) {
        return of(string, null);
    }

    public static MemorySegment of(ByteBuffer byteBuffer) {
        return MemorySegment.ofBuffer(byteBuffer);
    }

    public static MemorySegment of(String string, Charset charset) {
        return of(string.getBytes(charset == null ? StandardCharsets.UTF_8 : charset));
    }

    public static MemorySegment of(byte[] bytes) {
        return MemorySegment.ofBuffer(byteBuffer(bytes));
    }

    private MemorySegments() {
    }

    private static ByteBuffer byteBuffer(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.allocateDirect(bytes.length);
        bb.put(bytes);
        bb.flip();
        return bb;
    }
}
