package com.github.kjetilv.flopp.kernel.util;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.segments.LineSegmentMap;
import com.github.kjetilv.flopp.kernel.segments.LineSegmentMaps;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

class LineSegmentHashtableTest {

    @Test
    void foobar() {
        LineSegment foo = LineSegments.of("foo");
        LineSegment bar = LineSegments.of("bar");

        LineSegmentMap<Integer> table = LineSegmentMaps.create(16);

        table.put(foo, 42);
        table.put(bar, 47);

        LineSegmentMap<Integer> frozen = table.freeze();

        assertThat(frozen.get(foo)).isEqualTo(42);
        assertThat(frozen.get(bar)).isEqualTo(47);
        Map<LineSegment, Integer> frozenMap = frozen.toMap();
        assertThat(table.toMap()).isEqualTo(frozenMap);
    }

    @Test
    void toStringTest() {
        LineSegment foo = LineSegments.of("foo");
        LineSegment bar = LineSegments.of("bar");

        LineSegmentMap<Integer> table = LineSegmentMaps.create(16);

        table.put(foo, 42);
        table.put(bar, 47);

        String stringSorted = table.toStringSorted();

        TreeMap<String, Integer> treeMap = new TreeMap<>();
        treeMap.put("foo", 42);
        treeMap.put("bar", 47);
        assertThat(stringSorted).isEqualTo(treeMap.toString());
    }

    @Test
    void putAndGet() {
        LineSegment foo = LineSegments.of("foo");
        LineSegment bar = LineSegments.of("bar");

        LineSegmentMap<Integer> table = LineSegmentMaps.create(16);

        table.put(foo, 42);
        table.put(bar, 47);

        assertThat(table.get(foo)).isEqualTo(42);
        assertThat(table.get(bar)).isEqualTo(47);
    }

    @Test
    void putMergeGet() {
        LineSegment foo = LineSegments.of("foo");

        LineSegmentMap<Integer> table1 = LineSegmentMaps.create(16);
        table1.put(foo, 42);
        LineSegmentMap<Integer> table2 = LineSegmentMaps.create(16);
        table2.put(foo, 47);
        table1.merge(table2, Integer::sum);

        assertThat(table1.get(foo)).isEqualTo(42 + 47);
    }
}