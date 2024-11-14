package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.PartitionStreamer;
import com.github.kjetilv.flopp.kernel.SeparatedLine;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.kjetilv.flopp.kernel.files.LineSplitters.fwSink;
import static com.github.kjetilv.flopp.kernel.files.LineSplitters.fwTransform;

final class BitwiseFwSplitter extends AbstractPartitionedSplitter {

    private final Format.FwFormat format;

    BitwiseFwSplitter(PartitionStreamer streamer, Format.FwFormat format) {
        super(streamer);
        this.format = Objects.requireNonNull(format, "format");
    }

    @Override
    protected Consumer<LineSegment> consumer(Consumer<SeparatedLine> consumer) {
        return fwSink(format, consumer);
    }

    @Override
    protected Function<LineSegment, SeparatedLine> transform() {
        return fwTransform(format, null);
    }
}
