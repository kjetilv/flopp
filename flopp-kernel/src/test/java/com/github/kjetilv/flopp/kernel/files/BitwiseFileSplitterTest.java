package com.github.kjetilv.flopp.kernel.files;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.formats.Formats;
import com.github.kjetilv.flopp.kernel.partitions.Partitionings;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

@Disabled
class BitwiseFileSplitterTest {

    @Test
    void simple() throws IOException {
        Instant now = Instant.now();
        Set<String> airlines = new HashSet<>();
        try (Stream<Path> list = Files.list(PATH)) {
            list.filter(endsWith(".csv"))
                .parallel()
                .forEach(file -> {
                    try (Stream<String> lines = Files.lines(file)) {
                        lines.skip(1)
                            .forEach(line -> {
                                String[] split = line.split(",");
                                airlines.add(split[1]);
                            });
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        }
        System.out.println(airlines.size());
        System.out.println(airlines.stream().limit(10)
            .collect(Collectors.joining(", ")));
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    @Test
    void faster() throws IOException {
        Instant now = Instant.now();
        Set<String> airlines = new HashSet<>();
        Consumer<LineSegment> splitter = LineSplitters.Bitwise.csvSink(
            Formats.Csv.escape(),
            line ->
                airlines.add(line.column(1, UTF_8))
        );

        try (Stream<Path> list = Files.list(PATH)) {
            Path path = list.findFirst().orElseThrow();
            try (Stream<String> lines = Files.lines(path)) {
                lines.skip(1)
                    .map(string -> LineSegments.of(string, UTF_8))
                    .forEach(splitter);
            }
        }
        System.out.println(airlines.size());
        System.out.println(airlines.stream().limit(10)
            .collect(Collectors.joining(", ")));
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    @Test
    void fasterStill() {
        Instant now = Instant.now();
        Set<String> airlines = new HashSet<>();
        Path path = PATH;
        Consumer<LineSegment> splitter = LineSplitters.Bitwise.csvSink(
            Formats.Csv.escape(),
            line ->
                airlines.add(line.column(1, UTF_8))
        );
        try (
            Partitioned partititioned = PartitionedPaths.partitioned(path, Shape.of(path, UTF_8).header(2))
        ) {
            partititioned.streamers()
                .forEach(streamer ->
                    streamer.lines()
                        .forEach(splitter));
        }
        System.out.println(airlines.size());
        System.out.println(airlines.stream().limit(10)
            .collect(Collectors.joining(", ")));
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    @Test
    void jacksonCsv() throws IOException {
        Instant now = Instant.now();
        Set<String> airlines = new HashSet<>();
        CsvMapper mapper = new CsvMapper();
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);

        try (Stream<Path> list = Files.list(PATH)) {
            list.filter(endsWith(".csv"))
                .parallel()
                .forEach(file -> {
                    try {
                        try (
                            BufferedReader bufferedReader = Files.newBufferedReader(file);
                            MappingIterator<String[]> iterator = mapper.readerFor(String[].class)
                                .readValues(bufferedReader)
                        ) {
                            while (iterator.hasNext()) {
                                String[] row = iterator.next();
                                airlines.add(row[1]);
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
        }
        System.out.println(airlines.size());
        System.out.println(airlines.stream().limit(10)
            .collect(Collectors.joining(", ")));
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    @Test
    void jacksonCsvParallel() throws IOException {
        Set<String> airlines = new HashSet<>();
        CsvMapper mapper = new CsvMapper();
        mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);

        Instant now = Instant.now();
        List<CompletableFuture<Void>> voids;
        try (
            Stream<Path> list = Files.list(PATH);
            ExecutorService executor = new ForkJoinPool()
        ) {
            voids = list.filter(endsWith(".csv"))
                .parallel()
                .map(file ->
                    CompletableFuture.runAsync(
                        () -> {
                            try (
                                BufferedReader bufferedReader = Files.newBufferedReader(file);
                                MappingIterator<String[]> iterator = mapper.readerFor(String[].class)
                                    .readValues(bufferedReader)
                            ) {
                                while (iterator.hasNext()) {
                                    String[] row = iterator.next();
                                    airlines.add(row[1]);
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }, executor
                    ))
                .toList();
        }
        voids.forEach(CompletableFuture::join);

        System.out.println(airlines.size());
        System.out.println(airlines.stream().limit(10)
            .collect(Collectors.joining(", ")));
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    @Test
    void fasterStillParallel() {
        Set<String> airlines = new HashSet<>();
        Format.Csv.Escape format = Formats.Csv.escape(',', '\\');
        Consumer<SeparatedLine> lines = line -> airlines.add(line.column(1, UTF_8));
        Instant now = Instant.now();
        try (
            Stream<Path> list = Files.list(PATH);
            ExecutorService executor = new ForkJoinPool()
        ) {
            List<Partitioned> partitioneds = list.filter(endsWith(".csv"))
                .map(BitwiseFileSplitterTest::partitioned)
                .toList();
            List<CompletableFuture<Void>> futures = partitioneds.stream().flatMap(partititioned ->
                    partititioned.streamers()
                        .map(partitionStreamer ->
                            CompletableFuture.runAsync(
                                () ->
                                    partitionStreamer.lines()
                                        .forEach(LineSplitters.Bitwise.csvSink(format, lines)),
                                executor
                            )))
                .toList();
            futures.forEach(CompletableFuture::join);
            partitioneds.forEach(Partitioned::close);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(airlines.size());
        System.out.println(airlines.stream().limit(10)
            .collect(Collectors.joining(", ")));
        Duration time = Duration.between(now, Instant.now());
        System.out.println(time);
    }

    private static final Path PATH = Path.of(System.getProperty("csv.dir"));

    private static final Partitionings PARTITIONINGS = Partitionings.LONG;

    private static Partitioned partitioned(Path file) {
        return PartitionedPaths.partitioned(
            file,
            PARTITIONINGS.create().scaled(2),
            Shape.of(file, UTF_8).longestLine(1024).header(2)
        );
    }

    @SuppressWarnings("SameParameterValue")
    private static Predicate<Path> endsWith(String suffix) {
        return file ->
            file.getFileName().toString().endsWith(suffix);
    }
}
