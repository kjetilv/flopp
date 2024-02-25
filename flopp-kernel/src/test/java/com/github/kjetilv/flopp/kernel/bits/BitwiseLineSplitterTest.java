package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partitioned;
import com.github.kjetilv.flopp.kernel.Partitioning;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.assertj.core.api.Assertions.assertThat;

class BitwiseLineSplitterTest {

    @TempDir
    private Path tempDir;

    @Test
    void splitLine() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            new LinesFormat(';'),
            adder(splits)
        );
        LineSegment lineSegment = LineSegments.of("foo123;bar;234;abcdef;3456");
        splitter.accept(lineSegment);
        assertThat(splits).containsExactly(
            "foo123",
            "bar",
            "234",
            "abcdef",
            "3456"
        );
    }

    @Test
    void splitLineTwice() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            new LinesFormat(';'),
            adder(splits)
        );
        splitter.accept(LineSegments.of("foo123;bar;234;abcdef;3456"));
        assertThat(splits).containsExactly(
            "foo123",
            "bar",
            "234",
            "abcdef",
            "3456"
        );
        splits.clear();
        splitter.accept(LineSegments.of("foo123;bar;234;abcdef;3456"));
        assertThat(splits).containsExactly(
            "foo123",
            "bar",
            "234",
            "abcdef",
            "3456"
        );
    }

    @Test
    void shortString() {
        assertSplit(
            Partitioning.single(),
            "foo;bar;zot",
            "foo",
            "bar",
            "zot"
        );
    }

    @Test
    void shorterString() {
        assertSplit(
            Partitioning.single(), "foo;bar",
            "foo",
            "bar"
        );
    }

    @Test
    void shorterStringUTF8() {
        assertSplit(
            Partitioning.single(),
            """
                a;b
                åøøaåaåøøaåaåøøaåaåøøaåaåøøaåaåøøaåaåøø;0.1
                jk;kl
                """
        );
    }

    @Test
    void shorterStringUTF8Parts() {
        assertSplit(
            Partitioning.create(2),
            """
                a;b
                åøøaåaåøøaåaåøøaåaåøøaåaåøøaåaåøøaåaåøø;0.1
                jk;kl
                """
        );
    }

    @Test
    void shorterString8Plus() {
        assertSplit(Partitioning.single(), "fooz;barz");
    }

    @Test
    void shorterString8() {
        assertSplit(Partitioning.single(), "abcd;123");
    }

    @Test
    void shorterString8Short() {
        assertSplit(Partitioning.single(), "fooz;ba");
    }

    @Test
    void shorterStringProgressive() {
        assertSplit(
            Partitioning.single(), """
                f;a
                qweqweqweasdasdasdzxczxzxc;qwe
                a;qweqweqweasdasdasdzxczxzxc
                a;asd
                """
        );
    }

    @Test
    void veryShorterString() {
        assertSplit(
            Partitioning.single(), "a;b;c",
            "a",
            "b",
            "c"
        );
    }

    @Test
    void trickyString1() {
        assertSplit(
            Partitioning.single(), "'c';'';",
            "c", "", ""
        );
    }

    @Test
    void quoted() {
        assertSplit(
            Partitioning.single(), "'foo 1';bar;234;'ab; cd;ef';'it is \\'aight';;234;',';'\\;'",
            "foo 1",
            "bar",
            "234",
            "ab; cd;ef",
            "it is \\'aight",
            "",
            "234",
            ",",
            "\\;"
        );
    }

    @Test
    void quotedLimitedButNotReally() {
        List<String> splits = new ArrayList<>();
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            new LinesFormat(';', '\''),
            adder(splits)
        );
        splitter.accept(LineSegments.of(
            "'foo 1';bar;234;'ab; cd;ef';'it is \\'aight';;234;';;';'\\;'"));
        assertThat(splits).containsExactly(
            "foo 1", "bar", "234", "ab; cd;ef", "it is \\'aight", "", "234", ";;", "\\;");
    }

    @Test
    void quotedPickAll() {
        List<String> splits = new ArrayList<>();
        String input = "'foo 1';bar;234;'ab; cd;ef';'it is \\'aight';;234;';';'\\;'";
        String[] expected = {"foo 1", "bar", "234", "ab; cd;ef", "it is \\'aight", "", "234", ";", "\\;"};
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            new LinesFormat(';', '\''),
            adder(splits)
        );
        splitter.accept(LineSegments.of(
            input, StandardCharsets.UTF_8));
        assertThat(splits).containsExactly(
            expected);
    }

    @Test
    void splitFile() throws IOException {
        assertFileContents(
            """
                def123;cba;234;abcdef;3456
                abc234;foo;456;dfgfgh;1234
                foo;bar;zot
                foo;bar
                            
                            
                zot;
                moreStuff;1;2;3;4;5;6
                """,
            "def123",
            "cba",
            "234",
            "abcdef",
            "3456",
            "abc234",
            "foo",
            "456",
            "dfgfgh",
            "1234",
            "foo",
            "bar",
            "zot",
            "foo",
            "bar",
            "",
            "",
            "zot",
            "",
            "moreStuff",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6"
        );
    }

    @Test
    void quotesFiles() throws IOException {
        assertFileContents(
            """
                foo;'123; ok'
                'b; ar';42
                """,
            "foo", "123; ok", "b; ar", "42"

        );
    }

    @Test
    void trickyFile() throws IOException {
        assertFileContents(
            """
                '';
                'f;o;o';bar;zot
                123123123;234234234;345345345
                '1;2';3
                ;
                """,
            "",
            "",
            "f;o;o",
            "bar",
            "zot",
            "123123123",
            "234234234",
            "345345345",
            "1;2",
            "3",
            "",
            ""
        );
    }

    private Partitioned<Path> partitioned(Partitioning partitioning, String contents) {
        try {
            return Bitwise.partititioned(fileWith(contents), partitioning);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path fileWith(String contents) throws IOException {
        Path write = Files.writeString(
            tempDir.resolve(STR."\{UUID.randomUUID().toString()}.txt"),
            contents,
            CREATE
        );
        return write;
    }

    private void assertSplit(Partitioning partitioning, String input) {
        List<String> exp = Arrays.stream(input.split("\n"))
            .flatMap(line ->
                Arrays.stream(line.split(";")))
            .toList();

        List<String> splits = splits(partitioning, input);
        assertThat(splits).containsExactlyElementsOf(exp);
    }

    private void assertSplit(
        Partitioning partitioning,
        String input,
        String... expected
    ) {
        assertThat(splits(partitioning, input))
            .containsExactly(expected);
    }

    private List<String> splits(Partitioning partitioning, String input) {
        List<String> splits = new ArrayList<>();
        try {
            try (Partitioned<Path> partitioned = partitioned(partitioning, input)) {
                partitioned.streams()
                    .lineSplitters(new LinesFormat(';', '\''))
                    .forEach(consumer ->
                        consumer.accept(commaSeparatedLine ->
                            commaSeparatedLine.columns()
                                .forEach(splits::add)));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed: " + String.join("\n", splits), e);
        }
        return splits;
    }

    private static void assertFileContents(String contents, String... lines) throws IOException {
        List<String> splits = new ArrayList<>();
        Path path = Files.write(
            Files.createTempFile(UUID.randomUUID().toString(), ".txt"),
            List.of(
                contents
                    .split("\n")
            )
        );
        BitwiseLineSplitter splitter = new BitwiseLineSplitter(
            new LinesFormat(';', '\''),
            line ->
                line.columns()
                    .forEach(splits::add)
        );

        try {
            try (Partitioned<Path> partititioned = Bitwise.partititioned(path, Partitioning.single())) {
                partititioned.streams().streamers()
                    .forEach(streamer ->
                        streamer.lines()
                            .forEach(splitter));
            }
            if (lines.length > 0) {
                assertThat(splits).containsExactly(lines);
            } else {
                assertThat(splits).containsExactlyElementsOf(
                    Arrays.stream(contents.split("\n"))
                        .flatMap(s ->
                            Arrays.stream(s.split(";")))
                        .toList());
            }
        } catch (Exception e) {
            throw new IllegalStateException(
                STR."""
                   Failed with list:
                     \{String.join("\n  ", splits)}
                   """, e);
        }
    }

    private static List<String> lines(String... lines) {
        return Arrays.stream(lines)
            .map(line -> line.split(";"))
            .flatMap(Arrays::stream)
            .toList();
    }

    private static Consumer<CommaSeparatedLine> adder(List<String> splits) {
        return line ->
            line.columns()
                .forEach(splits::add);
    }
}
