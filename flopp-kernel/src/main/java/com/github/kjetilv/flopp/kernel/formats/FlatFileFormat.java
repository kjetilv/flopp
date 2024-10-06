package com.github.kjetilv.flopp.kernel.formats;

import java.nio.charset.Charset;

public sealed interface FlatFileFormat<F
    extends FlatFileFormat<F>>
    permits CsvFormat, FwFormat {

    Charset charset();

    F withCharset(Charset charset);
}
