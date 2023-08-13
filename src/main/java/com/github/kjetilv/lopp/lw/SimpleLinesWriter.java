package com.github.kjetilv.lopp.lw;

import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

public class SimpleLinesWriter implements LinesWriter {

    private final PrintWriter printWriter;

    public SimpleLinesWriter(Path target, Charset charset) {
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
