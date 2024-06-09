package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.util.Print;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public record Partitions(long total, List<Partition> partitions, long tail) {

    public Partitions(long total, List<Partition> partitions, long tail) {
        this.total = Non.negative(total, "total");
        this.partitions = Non.empty(partitions, "partitions")
            .stream().sorted()
            .toList();
        this.tail = tail > 0 ? partitions().getLast().length() : 0L;
        if (this.total != this.partitions.stream().mapToLong(Partition::length).sum()) {
            throw new IllegalArgumentException("Wrong total: " + total + ": " + partitions);
        }
    }

    public int size() {
        return partitions.size();
    }

    public Partition get(int index) {
        return partitions.get(index);
    }

    public Partition getFirst() {
        return partitions.getFirst();
    }

    public Partition getLast() {
        return partitions.getLast();
    }

    Partitions insertAtEnd(Partitions other) {
        long combinedTotal = total + other.total();
        if (tail == 0L) {
            return new Partitions(
                combinedTotal,
                combine(
                    partitions,
                    other.partitions()
                ),
                0L
            );
        }
        int size = partitions().size();
        return new Partitions(
            combinedTotal,
            combine(
                partitions().subList(0, size - 1),
                other.partitions(),
                partitions().subList(size - 1, size)
            ),
            tail
        );
    }

    @SafeVarargs
    private static List<Partition> combine(List<Partition>... partitionLists) {
        List<Partition> all = Arrays.stream(partitionLists).flatMap(Collection::stream)
            .toList();
        int count = all.size();
        return all.stream().reduce(
            new ArrayList<>(),
            (list, partition) -> {
                list.add(next(list, partition, count));
                return list;
            },
            (ps1, ps2) -> {
                throw new IllegalStateException(ps1 + " / " + ps2);
            }
        );
    }

    private static Partition next(
        ArrayList<Partition> list,
        Partition partition,
        int count
    ) {
        if (list.isEmpty()) {
            return new Partition(0, count, 0, partition.length());
        }
        Partition last = list.getLast();
        long nextOffset = last.offset() + last.length();
        return new Partition(list.size(), count, nextOffset, partition.length());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + Print.bigInt(total) +
               " -> " + partitions.size() + "{" +
               partitions.stream()
                   .map(Partition::toString)
                   .collect(Collectors.joining(" ")) +
               "} tail:" + tail +
               "]";
    }
}
