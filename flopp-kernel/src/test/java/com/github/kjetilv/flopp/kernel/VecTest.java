package com.github.kjetilv.flopp.kernel;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorSpecies;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.stream.IntStream;

import static java.nio.ByteOrder.nativeOrder;
import static java.nio.charset.StandardCharsets.UTF_8;

public class VecTest {

    @Test
    public void test() {
        String s = """
            foofoofoo
            foo
            foofoo
            foo
            foo
            
            barbar
            bar
            bar
            
            barbar
            bar
            bar
            
            zotzot
            zot
            zot
            
            zotzot
            zot
            zot
            """;
        byteDance(s);

//
//        System.out.println(bv);
//        System.out.println(HOLS);
//        ByteVector bvln = bv.and(HOLS);
//        System.out.println(bvln);
//        System.out.println(bv.eq(HOLS));
//        System.out.println(bvln.eq((byte)'\n'));
//        System.out.println(LN);
//
//        VectorMask<Byte> mask = bv.eq((byte) 0x0A);
//        System.out.println(mask);
//
    }

    public static final VectorSpecies<Long> L_SPECIES = LongVector.SPECIES_PREFERRED;

    public static final VectorSpecies<Byte> B_SPECIES = ByteVector.SPECIES_PREFERRED;

    public static final boolean __ = true;

    public static final boolean $$ = false;

    public static final ByteVector LNS = ByteVector.broadcast(B_SPECIES, (byte) '\n');

    public static final ByteVector HOLS = ByteVector.broadcast(B_SPECIES, (byte) 0x7F);

    public static final ByteVector ZERO = ByteVector.zero(B_SPECIES);

    public static final VectorMask<Byte> LN = VectorMask.fromLong(B_SPECIES, 0x0A0A0A0A0A0A0AL);

    private static final VectorMask<Byte>[] ZEROES_16 = zeroes(ByteVector.SPECIES_128);

    private static final VectorMask<Byte>[] ZEROES_32 = zeroes(ByteVector.SPECIES_256);

    private static final VectorMask<Byte>[] ZEROES_64 = zeroes(ByteVector.SPECIES_512);

    private static final VectorMask<Long>[] LONGS_2 = zeroes(LongVector.SPECIES_128);

    private static final VectorMask<Long>[] LONGS_4 = zeroes(LongVector.SPECIES_128);

    private static final VectorMask<Long>[] LONGS_8 = zeroes(LongVector.SPECIES_512);

    private static <T> VectorMask<T>[] zeroes(VectorSpecies<T> species) {
        return IntStream.range(0, species.length())
            .mapToObj(bit -> VectorMask.fromValues(species, onlyFalse(species, bit)))
            .<VectorMask<T>>toArray(VectorMask[]::new);
    }

    private static <T> boolean[] onlyFalse(VectorSpecies<T> species, int i) {
        boolean[] bs = new boolean[species.length()];
        Arrays.fill(bs, true);
        bs[i] = false;
        return bs;
    }

    private static void byteDance(String s) {
        MemorySegment memorySegment = MemorySegment.ofArray(s.getBytes(UTF_8));

        long loopBound = B_SPECIES.loopBound(s.length());
        int offset = 0;
        while (loopBound > offset + B_SPECIES.length()) {
            ByteVector vector = ByteVector.fromMemorySegment(B_SPECIES, memorySegment, offset, nativeOrder());
            VectorMask<Byte> mask = vector.eq((byte) '\n');
            while (mask.anyTrue()) {
                int l = mask.firstTrue();
                System.out.println(offset + l);
                mask = mask.and(ZEROES_16[l]);
            }
            offset += B_SPECIES.length();
        }
    }

    private static VectorMask<Byte> bmask(boolean... bs) {
        return VectorMask.fromValues(B_SPECIES, bs);
    }
}
