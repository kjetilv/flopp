package com.github.kjetilv.flopp.kernel;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.READ;
import static jdk.incubator.vector.VectorOperators.EQ;

@Disabled
public class VectorTest {

    @Test
    void test(@TempDir Path dir) throws IOException {
        Arena arena = Arena.ofConfined();

        Stream<String> content =
            Stream.concat(
                Stream.of("a", "bb", "c"),
                IntStream.range(1, 5)
                    .mapToObj(i ->
                        IntStream.range(1, 5).mapToObj(j -> {
                            String n = IntStream.range(j, j + i)
                                .map(x -> x % 10)
                                .mapToObj(Integer::toString)
                                .collect(Collectors.joining());
                            String l = IntStream.range(j, j + i)
                                .map(x -> x % 26)
                                .map(x -> (int) x + 'a')
                                .mapToObj(x -> (char) x)
                                .map(Object::toString)
                                .collect(Collectors.joining());
                            return STR."\{l}\{n}";
                        }))
                    .flatMap(Function.identity())
            );

        Path file = Files.writeString(
            dir.resolve(UUID.randomUUID() + ".bin"),
            content.collect(Collectors.joining("\n"))
        );

        System.out.println(new String(Files.readAllBytes(file)));

        FileChannel channel = FileChannel.open(file, READ);
        long size = Files.size(file);
        MemorySegment ms = channel.map(READ_ONLY, 0, 64, arena).asReadOnly();

        VectorSpecies<Byte> byteVectorSpecies = VectorShape.preferredShape().withLanes(BYTE_SPECIES.elementType());
        System.out.println(byteVectorSpecies.length());
        ByteVector byteVector = ByteVector.fromMemorySegment(byteVectorSpecies, ms, 0, ByteOrder.nativeOrder());
        VectorMask<Byte> compare = byteVector.compare(EQ, '\n');

        long aLong = compare.toLong();
        long semicolonPos = Long.numberOfTrailingZeros(aLong);
        long validMask = (-1L >>> -semicolonPos);
        System.out.println(aLong);
    }

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
}
