package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.util.Non;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.LongSupplier;

@SuppressWarnings("unused")
public record Shape(long size, long longestLine, Charset charset, Decor decor) {

    public static Shape of(Path file) {
        return of(file, null);
    }

    public static Shape of(Path file, Charset charset) {
        if (Files.isRegularFile(file)) {
            return size(sizeOf(file), charset);
        }
        throw new IllegalArgumentException("Not a file: " + file);
    }

    public static Shape size(long size, Charset charset) {
        return new Shape(size, charset);
    }

    public static Shape decor(int header, int footer, Charset charset) {
        return new Shape(-1, 0, charset, new Decor(header, footer));
    }

    public Shape(long size, Charset charset) {
        this(size, 0, charset, null);
    }

    public Shape(long size, Decor decor) {
        this(size, 0, null, decor);
    }

    public Shape(long size, Charset charset, Decor decor) {
        this(size, 0, charset, decor);
    }

    public Shape(long size, long longestLine, Charset charset, Decor decor) {
        this.size = Math.max(-1, size);
        this.charset = charset == null ? Charset.defaultCharset() : charset;
        this.decor = decor == null ? new Decor() : decor;
        this.longestLine = longestLine;
    }

    public Shape headerFooter(int header, int footer) {
        return new Shape(size, longestLine, charset, new Decor(header, footer));
    }

    public Shape header(int header) {
        return new Shape(size, longestLine, charset, new Decor(header));
    }

    public Shape charset(Charset charset) {
        return new Shape(size, longestLine, Objects.requireNonNull(charset, "charset"), decor);
    }

    public Shape iso8859_1() {
        return charset(StandardCharsets.ISO_8859_1);
    }

    public Shape sized(LongSupplier sizeSupplier) {
        return this.size < 0
            ? new Shape(sizeSupplier.getAsLong(), longestLine, charset(), decor())
            : this;
    }

    public int header() {
        return decor().header();
    }

    public int footer() {
        return decor().footer();
    }

    public boolean limitsLineLength() {
        return longestLine > 0;
    }

    public Shape longestLine(int longestLine) {
        return new Shape(size, longestLine, charset, decor);
    }

    public boolean hasOverhead() {
        return decor().size() > 0;
    }

    public record Decor(int header, int footer) {

        public Decor() {
            this(0);
        }

        public Decor(int header) {
            this(header, 0);
        }

        public Decor(int header, int footer) {
            this.header = Non.negative(header, "header");
            this.footer = Non.negative(footer, "footer");
        }

        public int size() {
            return header + footer;
        }
    }

    private static long sizeOf(Path file) {
        long size;
        try {
            size = Files.size(file);
        } catch (Exception e) {
            throw new IllegalArgumentException("Bad file:" + file, e);
        }
        return size;
    }
}
