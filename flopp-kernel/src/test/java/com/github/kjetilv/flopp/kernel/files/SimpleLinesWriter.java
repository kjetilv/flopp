package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.LinesWriter;

import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

class SimpleLinesWriter implements LinesWriter {

    private final PrintWriter printWriter;

    SimpleLinesWriter(Path target, Charset charset) {
        try {
            this.printWriter = new PrintWriter(Files.newBufferedWriter(target, charset), false);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open " + target, e);
        }
    }

    @Override
    public void accept(String line) {
        printWriter.println(line);
    }

    @Override
    public void close() {
        try {
            printWriter.close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close " + printWriter, e);
        }
    }
}
