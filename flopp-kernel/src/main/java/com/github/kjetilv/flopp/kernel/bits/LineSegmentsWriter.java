package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.io.LinesWriter;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.util.Non;

import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

class LineSegmentsWriter implements LinesWriter<Stream<LineSegment>> {

    private final RandomAccessFile randomAccessFile;

    private final FileChannel channel;

    private final Arena arena;

    private final long inMemorySize;

    private long offset;

    private long segmentOffset;

    private MemorySegment memorySegment;

    LineSegmentsWriter(Path target, long inMemorySize) {
        try {
            this.randomAccessFile = new RandomAccessFile(target.toFile(), "rw");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open " + target, e);
        }
        this.inMemorySize = Non.negative(inMemorySize, "inMemorySize");
        this.channel = this.randomAccessFile.getChannel();
        this.arena = Arena.ofAuto();

        memorySegment = nextSegment(0);
    }

    @Override
    public void accept(Stream<LineSegment> lineSegmentStream) {
        lineSegmentStream.forEach(segment -> {
            long srcOffset = 0L;
            long srcLeft = segment.length();
            while (true) {
                long remaining = inMemorySize - segmentOffset;
                if (remaining < srcLeft) {
                    if (remaining > 0) {
                        write(segment, srcOffset, remaining);
                        srcOffset += remaining;
                        srcLeft -= remaining;
                    }
                    cycle();
                } else {
                    write(segment, srcOffset, srcLeft);
                    return;
                }
            }
        });
    }

    @Override
    public void close() {
        try {
            channel.truncate(offset + segmentOffset);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to truncate " + channel, e);
        }
        try {
            channel.close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close " + channel, e);
        }
        try {
            randomAccessFile.close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close " + randomAccessFile, e);
        }
    }

    private void cycle() {
        this.offset += this.inMemorySize;
        this.memorySegment = nextSegment(this.offset);
        this.segmentOffset = 0;
    }

    private void write(LineSegment lineSegment, long srcOffset, long length) {
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

    private MemorySegment nextSegment(long offset) {
        try {
            return channel.map(READ_WRITE, offset, this.inMemorySize, this.arena);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open segment @ " + offset, e);
        }
    }
}
