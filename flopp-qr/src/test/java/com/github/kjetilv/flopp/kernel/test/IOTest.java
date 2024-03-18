package com.github.kjetilv.flopp.kernel.test;

import com.github.kjetilv.flopp.kernel.qr.Qrs;
import com.github.kjetilv.flopp.kernel.qr.ReadQr;
import com.github.kjetilv.flopp.kernel.qr.WriteQr;
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
            WriteQr<String> foo = Qrs.writer(
                string ->
                        sink.add(Thread.currentThread() + ": " + string),
                10,
                "foo")
        ) {
            ExecutorService readExecutor = Executors.newSingleThreadExecutor();
            ReadQr readQr = Qrs.reader(readExecutor);
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
