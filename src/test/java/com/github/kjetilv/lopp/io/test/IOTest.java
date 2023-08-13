package com.github.kjetilv.lopp.io.test;

import com.github.kjetilv.lopp.io.ReadQr;
import com.github.kjetilv.lopp.io.WriteQr;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class IOTest {

    @Test
    void test() {
        List<String> sink = new ArrayList<>();

        try (
            WriteQr<String> foo = WriteQr.create(
                string ->
                        sink.add(Thread.currentThread() + ": " + string),
                10,
                "foo")
        ) {
            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            ReadQr readQr = ReadQr.create(readExecutor);
            ExecutorService writeExecutor = Executors.newSingleThreadExecutor();
            CompletableFuture<Void> async = CompletableFuture.runAsync(foo, writeExecutor);

            Stream.of("a", "b", "c").forEach(foo);
            foo.awaitAllProcessed();

            writeExecutor.shutdown();

            readQr.streamStrings(sink.stream(), 10)
                    .forEach(string ->
                            System.out.println(Thread.currentThread() + " " + string));
            foo.close();
            async.join();
        }
    }
}
