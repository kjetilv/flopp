package com.github.kjetilv.lopp;

import com.github.kjetilv.lopp.io.ReadQr;
import com.github.kjetilv.lopp.io.WriteQr;
import com.github.kjetilv.lopp.lc.AsyncLineCounter;
import com.github.kjetilv.lopp.lc.IndexingLineCounter;
import com.github.kjetilv.lopp.lc.LineCounter;
import com.github.kjetilv.lopp.lc.SimpleLineCounter;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.lang.Character.isUpperCase;
import static java.lang.Character.toLowerCase;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("SameParameterValue")
public class SizeTest {

    private static final Logger log = LoggerFactory.getLogger(SizeTest.class);

    private ExecutorService readerExec;

    private ExecutorService writeExec;

    private Path path;

    private int sliceSize;

    private int partitions;

    private int ioQueueSize;

    private Path tempDirectory;

    private FileShape fileShape;

    private int linesCount;

    private int columnCount;

    private int scanResolution;

    @Test
    @Disabled
    void asyncCountLines() {
        FileShape fileShape;
        Partitioning partitioning = new Partitioning(10, 8192, 10);
        try {
            fileShape = new FileShape().fileSize(Files.size(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long straight = countLinesStraight(path);
        logTime(1, () -> {
            long start = System.nanoTime();
            LineCounter.Lines lines = new AsyncLineCounter(fileShape, partitioning, readerExec).scan(path);
            assertEquals(straight, lines.linesCount());
            return Duration.ofNanos(System.nanoTime() - start);
        });
    }

    @Test
    void countLinesStraight() {
        FileShape fileShape1;
        Partitioning partitioning = new Partitioning(10, 8192, 10);
        try {
            fileShape1 = new FileShape().fileSize(Files.size(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long straight = countLinesStraight(path);
        logTime(10, () -> {
            long start = System.nanoTime();
            long count = new SimpleLineCounter().count(path);
            assertEquals(straight, count);
            return Duration.ofNanos(System.nanoTime() - start);
        });
//        logTime(10, () -> {
//            long start = System.nanoTime();
//            long count = new AsyncLineCounter(fileShape1, partitioning, readerExec).count(path);
//            assertEquals(straight, count);
//            return Duration.ofNanos(System.nanoTime() - start);
//        });
        logTime(10, () -> {
            long start = System.nanoTime();
            long count = new IndexingLineCounter(partitioning, fileShape1).count(path);
            assertEquals(straight, count);
            return Duration.ofNanos(System.nanoTime() - start);
        });
    }

    @Disabled
    @Test
    void queueingReadersAndMultipleQueueingWriters(TestInfo testInfo) {
        logTime(20, () -> doQueueingReadersAndMultipleQueueingWriters(testInfo, null, OP));
    }

    @Test
    void realStuff(TestInfo testInfo) {
        logTime(20, () -> doRealStuff(testInfo, null, OP));
    }

    @Test
    void realStuffVerified(TestInfo testInfo) {
        logTime(1, () -> doRealStuffVerified(testInfo, null, OP));
    }

    @Test
    void realStuffOnce(TestInfo testInfo) {
        logTime(1, () -> doRealStuff(testInfo, null, OP));
    }

    @Test
    void simpleReadersMultipleSimpleWriters(TestInfo testInfo) {
        logTime(25, () -> doSimpleReadersAndMultipleSimpleWriters(testInfo, OP));
    }

    @Test
    void theStraightStory(TestInfo testInfo) {
        logTime(15, () -> doTheStraightStory(testInfo, OP));
    }

    @Test
    void theStraightStoryParallel(TestInfo testInfo) {
        logTime(15, () -> doTheStraightStoryParallel(testInfo, OP));
    }

    @Test
    void theStraightStoryQueued(TestInfo testInfo) {
        logTime(25, () -> doTheStraightStoryQueued(testInfo, OP));
    }

    @BeforeEach
    void setUp(TestInfo testInfo) {
        linesCount = 100_000;
        columnCount = 2;
        ioQueueSize = 10;
        scanResolution = 1;
        readerExec = writeExec = new ThreadPoolExecutor(
            32,
            64,
            1,
            TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(100)
        );
        sliceSize = 64 * 1024;

        int header = 2;
        int footer = 1;

        String className = testInfo.getTestClass()
            .map(Class::getSimpleName).orElseThrow();
        String methodName = testInfo.getTestMethod()
            .map(Method::getName).orElseThrow();
        String pathBase = className + "-" + methodName;
        try {
            tempDirectory = Files.createTempDirectory(pathBase + "-" + System.currentTimeMillis());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("Test: {}", tempDirectory.toUri());

        fileShape = FileShape.base().iso8859_1().header(header, footer);
        path = FileBuilder.file(tempDirectory, pathBase, fileShape, linesCount, columnCount);
        partitions = Runtime.getRuntime().availableProcessors();
    }

    @AfterEach
    void tearDown() {
        readerExec.shutdown();
        writeExec.shutdown();
        readerExec.shutdownNow();
        writeExec.shutdownNow();
        try {
            assertTrue(readerExec.awaitTermination(10, TimeUnit.SECONDS) && writeExec.awaitTermination(
                10,
                TimeUnit.SECONDS
            ));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        readerExec = null;
        writeExec = null;
    }

    private PartitionedFile partitionedFile(ExecutorService service) {
        return PartitionedFile.create(
            path,
            fileShape,
            new Partitioning(partitions, 8192, 1),
            service
        );
    }

    private Duration doQueueingReadersAndMultipleQueueingWriters(
        TestInfo testInfo,
        String qual,
        Function<String, String> r
    ) {
        long start = System.nanoTime();
        Path tmp = out(path, testInfo, qual);
        try (PartitionedFile partitionedFile = partitionedFile(readerExec)) {
            PathResultConsumer pathResultConsumer = new PathResultConsumer(partitionedFile.partitionCount());
            readerExec.submit(() -> partitionedFile.asyncMap(sliceSize, (partitionBytes, stream) -> {
                    Partition partition = partitionBytes.partition();
                    Path pbTemp = out(path, testInfo, partition, qual);
                    int index = partition.partitionNo();
                    try (PrintWriter pw = pw(pbTemp)) {
                        try (
                            WriteQr<String> writeQr = WriteQr.create("w-" + index, pw::println, ioQueueSize).in(writeExec);
                            Stream<NPLine> lines = ReadQr.create(readerExec)
                                .stream("r-" + index, stream, ioQueueSize, NPLine.empty())
                        ) {
                            lines.map(NPLine::line)
                                .map(reader(r))
                                .forEach(writeQr);
                            writeQr.awaitAllProcessed();
                        }
                    }
                    return pbTemp;
                }, readerExec)
                .forEach(pathResultConsumer));
            transfer(tmp, pathResultConsumer.stream());
        }
        Duration time = log(testInfo, start);
        checkLineCount(tmp);
        return time;
    }

    private Duration doRealStuff(TestInfo testInfo, String qual, Function<String, String> processor) {
        long start = System.nanoTime();
        Path tmp = out(path, testInfo, qual);
        try (
            PartitionedFile partitionedFile = PartitionedFile.create(
                path,
                fileShape,
                new Partitioning(partitions, 8192, scanResolution)
            )
        ) {
            partitionedFile.processSingle(processor, tmp, readerExec, sliceSize);
        }
        Duration time = log(testInfo, start);
        checkLineCount(tmp);
        return time;
    }

    private Duration doRealStuffVerified(TestInfo testInfo, String qual, Function<String, String> processor) {
        long start = System.nanoTime();
        Path tmp = out(path, testInfo, qual);
        Path verified = straightUp(testInfo, processor, "verified");
        try (
            PartitionedFile partitionedFile = PartitionedFile.create(
                path,
                fileShape,
                new Partitioning(partitions, 8192, scanResolution)
            )
        ) {
            partitionedFile.processSingle(processor, tmp, readerExec, sliceSize);
        }
        Duration time = log(testInfo, start);
        checkLineCount(tmp);
        assertThat(tmp).hasSameTextualContentAs(verified);
        return time;
    }

    private Duration doSimpleReadersAndMultipleSimpleWriters(TestInfo testInfo, Function<String, String> r) {
        Path tmp = out(path, testInfo);
        long start = System.nanoTime();
        try (PartitionedFile partitionedFile = partitionedFile(readerExec)) {
            PathResultConsumer pathResultConsumer = new PathResultConsumer(partitionedFile.partitionCount());
            PartitionBytesProcessor<Path> pathPartitionBytesProcessor = (partitionBytes, npLines) -> {
                Path subTmp = out(path, testInfo, partitionBytes.partition());
                try (PrintWriter pw = pw(subTmp)) {
                    npLines.map(NPLine::line)
                        .map(reader(r))
                        .forEach(pw::println);
                }
                return subTmp;
            };
            Stream<CompletableFuture<PartitionedFile.Result<Path>>> pathResults = partitionedFile.asyncMap(
                sliceSize,
                pathPartitionBytesProcessor,
                readerExec
            );
            readerExec.execute(() -> pathResults.forEach(pathResultConsumer));
            transfer(tmp, pathResultConsumer.stream());
        }
        Duration time = log(testInfo, start);
        checkLineCount(tmp);
        return time;
    }

    private Duration doTheStraightStory(TestInfo testInfo, Function<String, String> r) {
        long start = System.nanoTime();
        Path tmp = straightUp(testInfo, r, null);
        Duration time = log(testInfo, start);
        checkLineCount(tmp);
        return time;
    }

    private Path straightUp(TestInfo testInfo, Function<String, String> r, String qual) {
        Path tmp = out(path, testInfo, qual);
        try (PrintWriter pw = pw(tmp)) {
            try (Stream<String> stringStream = Files.lines(path)) {
                stringStream.skip(fileShape.decor().header()).limit(linesCount)
                    .map(reader(r))
                    .forEach(pw::println);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return tmp;
    }

    private Duration doTheStraightStoryParallel(TestInfo testInfo, Function<String, String> r) {
        long start = System.nanoTime();
        Path tmp = out(path, testInfo, (String) null);
        try (PrintWriter pw = pw(tmp)) {
            try (Stream<String> stringStream = Files.lines(path)) {
                stringStream.skip(fileShape.decor().header()).limit(linesCount)
                    .parallel()
                    .map(reader(r))
                    .forEach(pw::println);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        Duration time = log(testInfo, start);
        checkLineCount(tmp);
        return time;
    }

    private Duration doTheStraightStoryQueued(TestInfo testInfo, Function<String, String> r) {
        long start = System.nanoTime();
        Path tmp = out(path, testInfo);
        try (PrintWriter pw = pw(tmp)) {
            try (WriteQr<String> stringWriteQr = WriteQr.create(pw::println, ioQueueSize)) {
                writeExec.submit(stringWriteQr);
                try (
                    Stream<String> stringStream = ReadQr.create(readerExec).streamStrings(
                        Files.lines(path),
                        ioQueueSize
                    )
                ) {
                    stringStream.skip(fileShape.decor().header()).limit(linesCount)
                        .map(reader(r))
                        .forEach(stringWriteQr);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        Duration time = log(testInfo, start);
        checkLineCount(tmp);
        return time;
    }

    private void checkLineCount(Path tmp) {
        assertEquals(
            countLinesStraight(path),
            fileShape.decor().size() + countLinesStraight(tmp),
            "Bad line count in " + tmp.toUri()
        );
    }

    private Path out(Path path, TestInfo testInfo, Partition partition, String qual) {
        String name = testInfo.getTestMethod()
            .map(Method::getName).orElseThrow();
        Path tmp = Path.of(path.getFileName() + "-" + name + (qual == null ? "" : "-" + qual) + (partition == null
            ? ""
            : "-" + partition.partitionNo()) + ".txt");
        return tempDirectory.resolve(tmp);
    }

    private Path out(Path path, TestInfo testInfo) {
        return out(path, testInfo, null, null);
    }

    private Path out(Path path, TestInfo testInfo, Partition partition) {
        return out(path, testInfo, partition, null);
    }

    private Path out(Path path, TestInfo testInfo, String qual) {
        return out(path, testInfo, null, qual);
    }

    public static final Function<String, String> OP = line -> {
        try {
            Optional<BigInteger> tail = Arrays.stream(line.split(";")).skip(1)
                .map(BigInteger::new).reduce(BigInteger::add);
            Optional<BigInteger> total = Arrays.stream(line.split(";"))
                .map(BigInteger::new).reduce(BigInteger::add);
            return tail.flatMap(ta -> total.map(to -> to.subtract(ta)))
                .map(BigInteger::toString).orElse("0");
        } catch (Exception e) {
            throw new IllegalStateException("Failed", e);
        }
    };

    private static void transfer(Path tmp, Stream<PartitionedFile.Result<Path>> results) {
        try (
            RandomAccessFile target = new RandomAccessFile(tmp.toFile(), "rw");
            FileChannel channel = target.getChannel()
        ) {
            results.map(result -> {
                    Path path1 = result.result();
                    Partition partition = result.bytesPartition();
                    return new PartitionTransferer(path1, partition);
                })
                .<Runnable>map(transferer -> () -> transferer.accept(channel))
                .map(CompletableFuture::runAsync)
                .toList()
                .forEach(CompletableFuture::join);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Duration logTime(int times, Supplier<Duration> doer) {
        Duration duration = Duration.ZERO;
        Duration longest = Duration.ZERO;
        Duration shortest = Duration.ofDays(365);
        int samples = 0;
        for (int i = 0; i < times; i++) {
            Duration time;
            try {
                time = doer.get();
            } catch (Throwable e) {
                throw new IllegalStateException("Failed on run #" + (i + 1) + "/" + times, e);
            }
            if (times < 5 || i > 2) {
                duration = duration.plus(time);
                shortest = time.compareTo(shortest) < 0 ? time : shortest;
                longest = time.compareTo(longest) > 0 ? time : longest;
                samples++;
            }
        }
        Duration mean;
        if (samples > 10) {
            Duration accounted = duration.minus(shortest).minus(longest);
            mean = Duration.ofNanos(accounted.toNanos() / (samples - 2));
        } else {
            mean = Duration.ofNanos(duration.toNanos() / samples);
        }
        log.info("Average: {}", mean.truncatedTo(ChronoUnit.MILLIS));
        return mean;
    }

    private static Duration msSince(long pbStart) {
        return Duration.ofNanos(System.nanoTime() - pbStart);
    }

    private static PrintWriter pw(Path pbTemp) {
        try {
            return new PrintWriter(Files.newBufferedWriter(pbTemp, CREATE));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Function<String, String> reader(Function<String, String> op) {
        return line -> op.apply(line.endsWith("\r") ? line.substring(0, line.length() - 1) : line);
    }

    private static Duration log(TestInfo testInfo, long start) {
        Duration duration = msSince(start);
        String time = duration.truncatedTo(ChronoUnit.MILLIS).toString();
        String method = testInfo.getTestMethod()
            .map(Method::getName)
            .orElseThrow()
            .chars()
            .mapToObj(c ->
                isUpperCase(c) ?
                    " " + Character.toString(toLowerCase(c))
                    : Character.toString(c))
            .collect(joining());
        log.info("{}: {}", method, time);
        return duration;
    }

    private static long countLinesStraight(Path path) {
        try (Stream<String> lines = Files.lines(path, StandardCharsets.ISO_8859_1)) {
            return lines.count();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
