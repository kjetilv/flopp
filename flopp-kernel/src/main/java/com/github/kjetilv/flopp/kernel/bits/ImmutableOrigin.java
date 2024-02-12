package com.github.kjetilv.flopp.kernel.bits;

public record ImmutableOrigin(int file, long line, int column) implements Origin {
}
