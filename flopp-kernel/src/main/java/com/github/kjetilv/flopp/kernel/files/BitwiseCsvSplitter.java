package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.formats.Format;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import com.github.kjetilv.flopp.kernel.segments.SeparatedLine;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.kjetilv.flopp.kernel.files.LineSplitters.csvSink;
import static com.github.kjetilv.flopp.kernel.files.LineSplitters.csvTransform;

final class BitwiseCsvSplitter extends AbstractPartitionedSplitter {

    private final Format.Csv format;

    BitwiseCsvSplitter(PartitionStreamer streamer, Format.Csv format) {
        super(streamer);
        this.format = Objects.requireNonNull(format, "format");
    }

    @Override
    protected Consumer<LineSegment> consumer(Consumer<SeparatedLine> consumer) {
        return csvSink(format, consumer);
    }

    @Override
    protected Function<LineSegment, SeparatedLine> transform() {
        return csvTransform(format);
    }
}
