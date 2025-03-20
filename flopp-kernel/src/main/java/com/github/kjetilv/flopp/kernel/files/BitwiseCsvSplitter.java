package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.files.LineSplitters.Bitwise;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

final class BitwiseCsvSplitter extends AbstractBitwiseSplitter {

    private final Format.Csv format;

    BitwiseCsvSplitter(PartitionStreamer streamer, Format.Csv format) {
        super(streamer);
        this.format = Objects.requireNonNull(format, "format");
    }

    @Override
    protected Consumer<LineSegment> consumer(Consumer<SeparatedLine> consumer) {
        return Bitwise.csvSink(format, consumer);
    }

    @Override
    protected Function<LineSegment, SeparatedLine> transform() {
        return Bitwise.csvTransform(format);
    }
}
