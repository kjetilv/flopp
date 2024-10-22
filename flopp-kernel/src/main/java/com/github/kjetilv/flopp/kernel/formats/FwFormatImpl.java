package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.segments.Range;

import java.nio.charset.Charset;

record FwFormatImpl(Range[] ranges, Charset charset) implements Format.FwFormat {
}
