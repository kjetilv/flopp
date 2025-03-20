package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.SeparatedLine;
import com.github.kjetilv.flopp.kernel.files.LineSplitters.Vector;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

final class VectorFwSplitter extends AbstractVectorSplitter {

    private final Format.FwFormat format;

    VectorFwSplitter(PartitionStreamer streamer, Format.FwFormat format) {
        super(streamer);
        this.format = Objects.requireNonNull(format, "format");
    }

    @Override
    protected Consumer<LineSegment> consumer(Consumer<SeparatedLine> consumer) {
        return Vector.fwSink(format, consumer);
    }

    @Override
    protected Function<LineSegment, SeparatedLine> transform() {
        return Vector.fwTransform(format, null);
    }
}
