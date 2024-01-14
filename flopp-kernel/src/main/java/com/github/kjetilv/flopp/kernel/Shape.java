package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.LongSupplier;

@SuppressWarnings("unused")
public record Shape(long size, Charset charset, Decor decor, Stats stats) {

    public static Shape decor(int header, int footer) {
        return new Shape(-1, null, new Decor(header, footer), null);
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
            throw new IllegalArgumentException("Bad file: " + file, e);
        }
        throw new IllegalArgumentException("Not a file: " + file);
    }

    public Shape(long size) {
        this(size, null, null, null);
    }

    public Shape(long size, Charset charset, Decor decor, Stats stats) {
        this.size = Math.max(-1, size);
        this.charset = charset == null ? StandardCharsets.UTF_8 : charset;
        this.decor = decor == null ? new Decor() : decor;
        this.stats = stats;
    }

    public Shape(long size, Charset charset) {
        this(size, charset, null, null);
    }

    public Shape(long size, Decor decor) {
        this(size, null, decor, null);
    }

    public Shape(long size, Charset charset, Decor decor) {
        this(size, charset, decor, null);
    }

    public Shape header(int header, int footer) {
        return new Shape(size, charset, new Decor(header, footer), stats);
    }

    public Shape header(int header) {
        return new Shape(size, charset, new Decor(header), stats);
    }

    public Shape utf8() {
        return charset(StandardCharsets.UTF_8);
    }

    public Shape charset(Charset charset) {
        return new Shape(size, Objects.requireNonNull(charset, "charset"), decor, stats);
    }

    public Shape iso8859_1() {
        return charset(StandardCharsets.ISO_8859_1);
    }

    public Shape sized(LongSupplier sizeSupplier) {
        return this.size < 0
            ? new Shape(sizeSupplier.getAsLong(), charset(), decor(), stats())
            : this;
    }

    public int header() {
        return decor().header();
    }

    public int footer() {
        return decor().footer();
    }

    public boolean hasStats() {
        return stats != null;
    }

    public boolean limitsLineLength() {
        return stats != null && stats.limitsLineLength();
    }

    public Shape longestLine(int longestLine) {
        return longestLine(longestLine, false);
    }

    public Shape longestLineHardLimit(int longestLine) {
        return longestLine(longestLine, true);
    }

    public Shape longestLine(int longestLine, boolean hardLimit) {
        return new Shape(
            size,
            charset,
            decor,
            new Stats(longestLine, hardLimit)
        );
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

    public record Stats(
        int longestLine,
        boolean hardLimit
    ) {

        public Stats {
            Non.negative(longestLine, "longestLine");
        }

        public Stats hardLimit(boolean hardLimit) {
            return new Stats(longestLine, hardLimit);
        }

        public boolean limitsLineLength() {
            return hardLimit && longestLine > 0;
        }
    }
}
