package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class MemorySegments {

    static MemorySegment of(String string) {
        return of(string, null);
    }

    static MemorySegment of(ByteBuffer byteBuffer) {
        return MemorySegment.ofBuffer(byteBuffer);
    }

    static MemorySegment of(String string, Charset charset) {
        return of(string.getBytes(charset == null ? StandardCharsets.UTF_8 : charset));
    }

    static MemorySegment of(byte[] bytes) {
        return MemorySegment.ofBuffer(byteBuffer(bytes));
    }

    private static ByteBuffer byteBuffer(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.allocateDirect(bytes.length);
        bb.put(bytes);
        bb.flip();
        return bb;
    }

    private MemorySegments() {
    }
}
