package com.github.kjetilv.flopp.kernel.formats;

import java.nio.charset.Charset;

public sealed interface FlatFileFormat permits CsvFormat, FwFormat{

    Charset charset();
}
