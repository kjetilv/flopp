package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.io.LinesWriter;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.util.Non;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

class MemorySegmentLinesWriter implements LinesWriter<LineSegment> {

    private final RandomAccessFile randomAccessFile;

    private final FileChannel channel;

    private final Arena arena;

    private final long inMemorySize;

    private long offset;

    private long segmentOffset;

    private MemorySegment memorySegment;

    MemorySegmentLinesWriter(Path target, long inMemorySize) {
        try {
            this.randomAccessFile = new RandomAccessFile(target.toFile(), "rw");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open " + target, e);
        }
        this.inMemorySize = Non.negative(inMemorySize, "inMemorySize");
        this.channel = this.randomAccessFile.getChannel();
        this.arena = Arena.ofAuto();

        memorySegment = nextSegment();
    }

    @Override
    public void accept(LineSegment lineSegment) {
        long length = lineSegment.length();
        if (segmentOffset + length < inMemorySize) {
            copy(lineSegment, 0, length);
            return;
        }

        long available = inMemorySize - segmentOffset;
        copy(lineSegment, 0, available);

        long remaining = length - available;
        this.memorySegment = nextSegment();
        copy(lineSegment, available, remaining);
    }

    @Override
    public void close() {
        try {
            arena.close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close " + arena, e);
        }
        try {
            channel.close();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close " + channel, e);
        }
        try {
            randomAccessFile.close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close " + randomAccessFile, e);
        }
    }

    private void copy(LineSegment lineSegment, long srcOffset, long length) {
        MemorySegment src = lineSegment.memorySegment();
        MemorySegment.copy(
            src,
            srcOffset,
            memorySegment,
            segmentOffset,
            length
        );
        this.segmentOffset += length;
    }

    private MemorySegment nextSegment() {
        try {
            return channel.map(FileChannel.MapMode.READ_WRITE, offset, this.inMemorySize, this.arena);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open segment @ " + offset, e);
        } finally {
            offset += this.inMemorySize;
        }
    }
}
