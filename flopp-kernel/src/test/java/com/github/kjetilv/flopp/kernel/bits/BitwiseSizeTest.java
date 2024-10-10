package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.segments.LineSegment;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.lang.Character.isUpperCase;
import static java.lang.Character.toLowerCase;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("SameParameterValue")
public class BitwiseSizeTest {

    private ExecutorService readerExec;

    private Path path;

    private int partitions;

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private int ioQueueSize;

    @TempDir
    private Path tempDirectory;

    private Shape shape;

    private int linesCount;

    @SuppressWarnings("FieldCanBeLocal")
    private int columnCount;

    @Disabled
    @Test
    void realStuff(TestInfo testInfo) {
        logTime(100, () -> doRealStuff(testInfo, null, OP));
    }

    @Test
    void realStuffVerified(TestInfo testInfo) {
        logTime(1, () -> doRealStuffVerified(testInfo, null, OP));
    }

    @Test
    void realStuffOnce(TestInfo testInfo) {
        logTime(1, () -> doRealStuff(testInfo, null, OP));
    }

    @Disabled
    @Test
    void realStuffFastOnce(TestInfo testInfo) {
        logTime(1, () -> doRealStuffFast(testInfo, null, OP));
    }

    @Disabled
    @Test
    void theStraightStory(TestInfo testInfo) {
        logTime(100, () -> doTheStraightStory(testInfo, OPS));
    }

    @Disabled
    @Test
    void theStraightStoryParallel(TestInfo testInfo) {
        logTime(50, () -> doTheStraightStoryParallel(testInfo, OPS));
    }

    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException {
        linesCount = 1_000_000;
        columnCount = 10;
        ioQueueSize = 10;
        readerExec = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 10, r -> {
                Thread thread = new Thread(r);
                thread.setUncaughtExceptionHandler((_, e) -> e.printStackTrace(System.err));
                return thread;
            }
        );

        int header = 3;
        int footer = 2;

        String className = testInfo.getTestClass()
            .map(Class::getSimpleName).orElseThrow();
        String methodName = testInfo.getTestMethod()
            .map(Method::getName).orElseThrow();
        String pathBase = className + "-" + methodName;
        System.out.printf("Test: %s%n", tempDirectory.toUri());

        path = FileBuilder.file(tempDirectory, pathBase, linesCount, columnCount, new Shape.Decor(header, footer));
        shape = Shape.size(Files.size(path), UTF_8).headerFooter(header, footer).longestLine(256);
        partitions = Runtime.getRuntime().availableProcessors();
    }

    @AfterEach
    void tearDown() {
        readerExec.shutdown();
        readerExec.shutdownNow();
        try {
            assertTrue(readerExec.awaitTermination(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        readerExec = null;
    }

    private Duration doRealStuff(TestInfo testInfo, String qual, Function<LineSegment, String> fun) {
        long start = System.nanoTime();
        Path tmp = out(path, testInfo, qual);
        Partitioning partitioning = Partitioning.create(partitions);
        try (
            Partitioned<Path> partitioned = PartitionedPaths.partitioned(path, partitioning, shape);
            PartitionedProcessor<Path, LineSegment, String> processor =
                PartitionedPaths.processor(partitioned, shape.charset())
        ) {
            processor.process(tmp, fun);
        }
        return log(testInfo, start);
    }

    @SuppressWarnings("unused")
    private Duration doRealStuffQr(
        TestInfo testInfo,
        String qual,
        Function<LineSegment, String> processor,
        int bufferSize
    ) {
        long start = System.nanoTime();
        Path tmp = out(path, testInfo, qual);
        Partitioning partitioning = Partitioning.create(partitions, bufferSize);
        try (
            Partitioned<Path> partitioned = PartitionedPaths.partitioned(path, partitioning, shape);
            PartitionedProcessor<Path, LineSegment, String> lineProcessor =
                PartitionedPaths.processor(partitioned, shape.charset())
        ) {
            lineProcessor.process(tmp, processor);
        }
        return log(testInfo, start);
    }

    private Duration doRealStuffFast(TestInfo testInfo, String qual, Function<LineSegment, String> fun) {
        Path tmp = out(path, testInfo, qual);
        long start = System.nanoTime();
        Partitioning partitioning = Partitioning.create(partitions, 8192);
        try (
            Partitioned<Path> partitioned = PartitionedPaths.partitioned(path, partitioning, shape);
            PartitionedProcessor<Path, LineSegment, String> processor =
                PartitionedPaths.processor(partitioned, shape.charset())
        ) {
            processor.process(tmp, fun);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return log(testInfo, start);
    }

    private Duration doRealStuffVerified(TestInfo testInfo, String qual, Function<LineSegment, String> fun) {
        Path verified = straightUp(testInfo, OPS, "verified");
        Path out = out(path, testInfo, qual);
        long start = System.nanoTime();
        Partitioning partitioning = Partitioning.create(partitions, shape.longestLine());
        try (
            Partitioned<Path> partitioned = PartitionedPaths.partitioned(path, partitioning, shape);
            PartitionedProcessor<Path, LineSegment, String> processor =
                PartitionedPaths.processor(partitioned, shape.charset())
        ) {
            processor.process(out, fun);
        }
        Duration time = log(testInfo, start);
        assertThat(out).hasSameTextualContentAs(verified);
        return time;
    }

    private Duration doTheStraightStory(TestInfo testInfo, Function<String, String> r) {
        long start = System.nanoTime();
        straightUp(testInfo, r, null);
        return log(testInfo, start);
    }

    private Path straightUp(TestInfo testInfo, Function<String, String> r, String qual) {
        Path tmp = out(path, testInfo, qual);
        try (PrintWriter pw = pw(tmp)) {
            try (Stream<String> stringStream = Files.lines(path)) {
                stringStream.skip(shape.decor().header()).limit(linesCount)
                    .map(reader(r))
                    .forEach(pw::println);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return tmp;
    }

    private Duration doTheStraightStoryParallel(TestInfo testInfo, Function<String, String> r) {
        Path tmp = out(path, testInfo, null);
        long start = System.nanoTime();
        try (PrintWriter pw = pw(tmp)) {
            try (Stream<String> stringStream = Files.lines(path)) {
                stringStream.skip(shape.decor().header()).limit(linesCount)
                    .parallel()
                    .map(reader(r))
                    .forEach(pw::println);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return log(testInfo, start);
    }

    private Path out(Path path, TestInfo testInfo, Partition partition, String qual) {
        String name = testInfo.getTestMethod()
            .map(Method::getName).orElseThrow();
        return tempDirectory.resolve(
            Path.of(path.getFileName() + "-" + name +
                    (qual == null ? ""
                        : "-" + qual +
                          (partition == null ? ""
                              : "-" + partition.partitionNo()) + ".txt")));
    }

    private Path out(Path path, TestInfo testInfo, String qual) {
        return out(path, testInfo, null, qual);
    }

    @SuppressWarnings("CommentedOutCode")
    public static final Function<String, String> OPS = line -> {
        try {
            String[] split = line.split(";");
//            Optional<BigInteger> tail = Arrays.stream(split).skip(1)
//                .map(BigInteger::new).reduce(BigInteger::add);
//            Optional<BigInteger> total = Arrays.stream(split)
//                .map(BigInteger::new).reduce(BigInteger::add);
//            Optional<BigInteger> bigInteger = tail
//                .flatMap(ta ->
//                    total.map(to -> to.subtract(ta)));
            if (split.length > 1) {
                return new BigDecimal(split[1]).toPlainString();
            }
            return "0";
//            return Arrays.stream(split).findFirst()
//                .map(BigInteger::new)
//                .map(BigInteger::toString)
//                .orElse("0");
        } catch (Exception e) {
            throw new IllegalStateException("Failed", e);
        }
    };

    private static final Function<LineSegment, String> TOS =
        lineSegment -> lineSegment.asString(UTF_8);

    public static final Function<LineSegment, String> OP = TOS.andThen(OPS);

    private static void logTime(int times, Supplier<Duration> doer) {
        Duration duration = Duration.ZERO;
        Duration longest = Duration.ZERO;
        Duration shortest = Duration.ofDays(365);
        int samples = 0;
        for (int i = 0; i < times; i++) {
            Duration time;
            try {
                time = doer.get();
            } catch (Throwable e) {
                throw new IllegalStateException("Failed on run #" + (i + 1) + " " + times, e);
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
        System.out.printf("Average: %s%n", mean.truncatedTo(ChronoUnit.MILLIS));
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
        System.out.printf("%s: %s%n", method, time);
        return duration;
    }

}
