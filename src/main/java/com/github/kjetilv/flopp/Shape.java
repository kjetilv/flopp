package com.github.kjetilv.flopp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@SuppressWarnings("unused")
public record Shape(long size, Charset charset, Decor decor, Stats stats) {

    public static Shape size(long size) {
        return new Shape(size);
    }

    public Shape(long size) {
        this(size, null, null, null);
    }

    public Shape(long size, Charset charset, Decor decor, Stats stats) {
        this.size = Non.negative(size, "size");
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

    public int header() {
        return decor().header();
    }

    public int footer() {
        return decor().footer();
    }

    public boolean hasStats() {
        return stats != null;
    }

    public Shape longestLine(int longestLine) {
        return new Shape(
            size,
            charset,
            decor,
            new Stats(longestLine)
        );
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
        int longestLine
    ) {

        public Stats(int longestLine) {
            this.longestLine = Non.negative(longestLine, "longestLine");
        }
    }
}
