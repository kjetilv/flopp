package com.github.kjetilv.lopp;

import com.github.kjetilv.lopp.utils.Non;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@SuppressWarnings("unused")
public record FileShape(Charset charset, Decor decor, Stats stats) {

    public FileShape() {
        this(null, null, null);
    }

    public FileShape(Charset charset, Decor decor, Stats stats) {
        this.charset = charset == null ? StandardCharsets.UTF_8 : charset;
        this.decor = decor == null ? new Decor() : decor;
        this.stats = stats;
    }

    public FileShape(Charset charset) {
        this(charset, null, null);
    }

    public FileShape(Decor decor) {
        this(null, decor, null);
    }

    public FileShape(Charset charset, Decor decor) {
        this(charset, decor, null);
    }

    public static FileShape base() {
        return new FileShape();
    }

    public FileShape header(int header, int footer) {
        return new FileShape(
            charset,
            new Decor(header, footer),
            stats
        );
    }

    public FileShape header(int header) {
        return new FileShape(charset, new Decor(header), stats);
    }

    public FileShape utf8() {
        return charset(StandardCharsets.UTF_8);
    }

    public FileShape charset(Charset charset) {
        return new FileShape(Objects.requireNonNull(charset, "charset"), decor, stats);
    }

    public FileShape iso8859_1() {
        return charset(StandardCharsets.ISO_8859_1);
    }

    public int header() {
        return decor().header();
    }

    public int footer() {
        return decor().footer();
    }

    public FileShape stats(long linesCount, int longestLine) {
        if (stats() == null) {
            throw new IllegalStateException("No stats: " + this);
        }
        return new FileShape(
            charset(),
            decor(),
            stats().stats(linesCount, longestLine));
    }

    public FileShape fileSize(long fileSize) {
        return new FileShape(
            charset(),
            decor(),
            stats() == null ? new Stats(fileSize) : stats().fileSize(fileSize)
        );
    }

    public boolean hasStats() {
        return stats != null;
    }

    public long fileSize() {
        return stats().fileSize();
    }

    public FileShape longestLine(int longestLine) {
        return new FileShape(
            charset,
            decor,
            stats.longestLine(longestLine)
        );
    }

    public record Decor(int header, int footer) {

        public Decor() {
            this(0, 0);
        }

        public Decor(int header, int footer) {
            this.header = Non.negative(header, "header");
            this.footer = Non.negative(footer, "footer");
        }

        public Decor(int header) {
            this(header, 0);
        }

        public int size() {
            return header + footer;
        }
    }

    public record Stats(
        long fileSize,
        boolean uniformishLineLengths,
        long linesCount,
        int longestLine
    ) {

        public Stats(long fileSize) {
            this(fileSize, false);
        }

        public Stats(long fileSize, boolean uniformishLineLengths) {
            this(fileSize, uniformishLineLengths, -1, -1);
        }

        public Stats(long fileSize, long linesCount, int longestLine) {
            this(fileSize, false, linesCount, longestLine);
        }

        public Stats(long fileSize, boolean uniformishLineLengths, long linesCount, int longestLine) {
            this.fileSize = Non.negativeOrZero(fileSize, "fileSize");
            this.uniformishLineLengths = uniformishLineLengths;
            this.linesCount = linesCount;
            this.longestLine = longestLine;
        }

        public Stats longestLine(int longestLine) {
            return new Stats(fileSize, uniformishLineLengths, linesCount, longestLine);
        }

        Stats fileSize(long fileSize) {
            return new Stats(fileSize, uniformishLineLengths, linesCount, longestLine);
        }

        Stats stats(long linesCount, int longestLine) {
            return new Stats(fileSize, uniformishLineLengths, linesCount, longestLine);
        }
    }
}
