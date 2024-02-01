package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.LongSupplier;

@SuppressWarnings("unused")
public record Shape(long size, Charset charset, Decor decor, long longestLine) {

    public static Shape decor(int header, int footer) {
        return new Shape(-1, null, new Decor(header, footer), 0);
    }

    public static Shape size(long size) {
        return new Shape(size);
    }

    public static Shape of(Path file) {
        try {
            if (Files.isRegularFile(file)) {
                return size(Files.size(file));
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(STR."Bad file: \{file}", e);
        }
        throw new IllegalArgumentException(STR."Not a file: \{file}");
    }

    public Shape(long size) {
        this(size, null, null, 0);
    }

    public Shape(long size, Charset charset, Decor decor, long longestLine) {
        this.size = Math.max(-1, size);
        this.charset = charset == null ? StandardCharsets.UTF_8 : charset;
        this.decor = decor == null ? new Decor() : decor;
        this.longestLine = longestLine;
    }

    public Shape(long size, Charset charset) {
        this(size, charset, null, 0);
    }

    public Shape(long size, Decor decor) {
        this(size, null, decor, 0);
    }

    public Shape(long size, Charset charset, Decor decor) {
        this(size, charset, decor, 0);
    }

    public Shape headerFooter(int header, int footer) {
        return new Shape(size, charset, new Decor(header, footer), longestLine);
    }

    public Shape header(int header) {
        return new Shape(size, charset, new Decor(header), longestLine);
    }

    public Shape utf8() {
        return charset(StandardCharsets.UTF_8);
    }

    public Shape charset(Charset charset) {
        return new Shape(size, Objects.requireNonNull(charset, "charset"), decor, longestLine);
    }

    public Shape iso8859_1() {
        return charset(StandardCharsets.ISO_8859_1);
    }

    public Shape sized(LongSupplier sizeSupplier) {
        return this.size < 0
            ? new Shape(sizeSupplier.getAsLong(), charset(), decor(), longestLine)
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
        return new Shape(size, charset, decor, longestLine);
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
}
