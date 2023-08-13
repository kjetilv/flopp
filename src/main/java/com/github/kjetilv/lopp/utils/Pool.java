package com.github.kjetilv.lopp.utils;

import java.nio.ByteBuffer;

public interface Pool<T> {

    static Pool<ByteBuffer> byteBuffers(int length) {
        return new QueuePool<>(
                () ->
                        ByteBuffer.allocate(length),
                ByteBuffer::clear
        );
    }

    static Pool<byte[]> byteArrays(int length) {
        return new QueuePool<>(() -> new byte[length]);
    }

    T acquire();

    void release(T t);
}
