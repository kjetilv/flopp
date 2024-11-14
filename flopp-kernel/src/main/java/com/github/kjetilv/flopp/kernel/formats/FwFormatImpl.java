package com.github.kjetilv.flopp.kernel.formats;

import com.github.kjetilv.flopp.kernel.Format;
import com.github.kjetilv.flopp.kernel.Range;

import java.nio.charset.Charset;

public record FwFormatImpl(Range[] ranges, Charset charset) implements Format.FwFormat {
}
