package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.*;
import com.github.kjetilv.flopp.kernel.formats.Formats;
import com.github.kjetilv.flopp.kernel.partitions.Partitioning;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.assertj.core.api.Assertions.assertThat;

class CsvQuotedSplitterTest {

    @TempDir
    private Path tempDir;

    @Test
    void splitLine() {
        List<String> splits = new ArrayList<>();
        Consumer<LineSegment> splitter = LineSplitters.csvSink(
            Formats.Csv.quoted(';', '"'),
            adder(splits)
        );
        LineSegment lineSegment = LineSegments.of("foo123;bar;234;abcdef;3456", UTF_8);
        splitter.accept(lineSegment);
        assertThat(splits).containsExactly("foo123", "bar", "234", "abcdef", "3456");
    }

    @Test
    void splitLineTwice() {
        List<String> splits = new ArrayList<>();
        Consumer<LineSegment> splitter = LineSplitters.csvSink(
            Formats.Csv.quoted(';', '"'),
            adder(splits)
        );
        splitter.accept(LineSegments.of("foo123;bar;234;abcdef;3456", UTF_8));
        assertThat(splits).containsExactly("foo123", "bar", "234", "abcdef", "3456");
        splits.clear();
        splitter.accept(LineSegments.of("foo123;bar;234;abcdef;3456", UTF_8));
        assertThat(splits).containsExactly("foo123", "bar", "234", "abcdef", "3456");
    }

    @Test
    void shortString() {
        assertSplit(Partitioning.single(), "foo;bar;zot", "foo", "bar", "zot");
    }

    @Test
    void shorterString() {
        assertSplit(Partitioning.single(), "foo;bar", "foo", "bar");
    }

    @Test
    void shorterString2() {
        assertSplit(Partitioning.single(), "foo;'b'ar", "foo", "'b'ar");
    }

    //    @Disabled("Should fail")
    @Test
    void badQuoteString() {
        assertSplit(Partitioning.single(), "f;'b;a;", "f", "'b;a;");
    }

    @Test
    void dodgyQuoteString() {
        assertSplit(Partitioning.single(), "f;a'b;'a;", "f", "a'b;'a", "");
    }

    @Test
    void dodgyQuoteString2() {
        assertSplit(Partitioning.single(), "f;''a'b';'a;", "f", "''a'b'", "'a;");
    }

    @Test
    void shorterStringUTF8() {
        assertSplit(
            Partitioning.single(), """
                a;b
                åøøaåaåøøaåaåøøaåaåøøaåaåøøaåaåøøaåaåøø;0.1
                jk;kl
                """
        );
    }

    @Test
    void shorterStringUTF8Parts() {
        assertSplit(
            Partitioning.create(2), """
                a;b
                åøøaåaåøøaåaåøøaåaåøøaåaåøøaåaåøøaåaåøø;0.1
                jk;kl
                """
        );
    }

    @Test
    void shorterStringUTF8Parts2() {
        Partitioning partitioning = Partitioning.create(2);

//        String input = """
//            890000000000000000,MAYLUUSSTR,#topl RT jfrblazer49: I THINK JUDGE ROY MOORE WOULD BE AWESOME A SUPREME COURT JUDGE . GOD BLESS JUDGE ROY MOORE! https://t.co/qUSgEIoA1m …,Unknown,English,8/5/2017 6:21,8/5/2017 6:21,2965,638,1234,QUOTE_TWEET,Right,1,RightTroll,0,890467190402686977,893718554687811584,http://twitter.com/890467190402686977/statuses/893718554687811584,https://twitter.com/RebeccaFaussett/status/891646467949121539,,
//            890000000000000000,MAYLUUSSTR,#topl Moochie thinks she's sexy huh ? face like bull dog ass like a 57 Buick #may,Unknown,English,8/5/2017 6:21,8/5/2017 6:21,2965,638,1233,,Right,0,RightTroll,0,890467190402686977,893718549100978177,http://twitter.com/890467190402686977/statuses/893718549100978177,,,
//            890000000000000000,MAYLUUSSTR,"#topl RT AwakenToGod: Say often, ""I AM filled, surrounded, permeated, and pervaded by the pure, gentle, unconditional love that God is."" …",Unknown,English,8/5/2017 6:26,8/5/2017 6:37,2965,638,1236,,Right,0,RightTroll,0,890467190402686977,893719856440332288,http://twitter.com/890467190402686977/statuses/893719856440332288,,,
//            890000000000000000,MAYLUUSSTR,#topl RT tradingpsych01: #Violent Muslim refugees causing Chaos in Europe https://t.co/eNkgioQeHb #may,Unknown,English,8/5/2017 6:26,8/5/2017 6:26,2965,638,1237,,Right,0,RightTroll,0,890467190402686977,893719858478821376,http://twitter.com/890467190402686977/statuses/893719858478821376,http://bit.ly/1TJdmcH,,
//            """;
        String input2 = """
            890000000000000000,MAYLUUSSTR,"#topl RT AwakenToGod: Say often, ""I AM filled, surrounded, permeated, and pervaded by the pure, gentle, unconditional love that God is."" …",Unknown,English,8/5/2017 6:26,8/5/2017 6:37,2965,638,1236,,Right,0,RightTroll,0,890467190402686977,893719856440332288,http://twitter.com/890467190402686977/statuses/893719856440332288,,,
            """;
        List<String> splits1 = new ArrayList<>();
        try {
            try (Partitioned partitioned = partitioned(partitioning, input2)) {
                partitioned.splitters(Formats.Csv.quoted(',', '"'))
                    .forEach(consumer -> consumer.forEach(commaSeparatedLine -> commaSeparatedLine.columns(UTF_8)
                        .forEach(splits1::add)));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed: " + String.join("\n", splits1), e);
        }

        splits1.stream()
            .map(s -> "`" + s + "`")
            .forEach(System.out::println);
    }

    /*
890000000000000000,MAYLUUSSTR,#topl RT jfrblazer49: I THINK JUDGE ROY MOORE WOULD BE AWESOME A SUPREME COURT JUDGE . GOD BLESS JUDGE ROY MOORE! https://t.co/qUSgEIoA1m …,Unknown,English,8/5/2017 6:21,8/5/2017 6:21,2965,638,1234,QUOTE_TWEET,Right,1,RightTroll,0,890467190402686977,893718554687811584,http://twitter.com/890467190402686977/statuses/893718554687811584,https://twitter.com/RebeccaFaussett/status/891646467949121539,,
890000000000000000,MAYLUUSSTR,#topl Moochie thinks she's sexy huh ? face like bull dog ass like a 57 Buick #may,Unknown,English,8/5/2017 6:21,8/5/2017 6:21,2965,638,1233,,Right,0,RightTroll,0,890467190402686977,893718549100978177,http://twitter.com/890467190402686977/statuses/893718549100978177,,,
     */
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
        assertSplit(Partitioning.single(), "a;b;c", DQ_FORMAT, "a", "b", "c");
    }

    @Test
    void trickyString1() {
        assertSplit(
            Partitioning.single(),
            "'c';'';",
            DQ_FORMAT,
            "'c'",
            "''",
            ""
        );
    }

    @Test
    void trickyString2() {
        assertSplit(
            Partitioning.single(),
            "'c';'';'sdf'",
            DQ_FORMAT,
            "'c'",
            "''",
            "'sdf'"
        );
    }

    @Test
    void quoted() {
        assertSplit(
            Partitioning.single(),
            "'foo 1';bar;234;'ab; cd;ef';'it is ''aight';;234;',';'\\;'",
            DQ_FORMAT,
            "'foo 1'",
            "bar",
            "234",
            "'ab; cd;ef'",
            "'it is ''aight'",
            "",
            "234",
            "','",
            "'\\;'"
        );
    }

    @Test
    void quotingQuotes() {
        assertSplit(
            Partitioning.single(),
            """
                'foo 1';'it''s';''foo'';'''''';4
                """,
            DQ_FORMAT,
            "'foo 1'",
            "'it''s'",
            "''foo''",
            "''''''",
            "4"
        );
    }

    @Test
    void quotedLimitedButNotReally() {
        List<String> splits = new ArrayList<>();
        Consumer<LineSegment> splitter = LineSplitters.csvSink(DQ_FORMAT, adder(splits));
        splitter.accept(LineSegments.of(
            "'foo 1';bar;234;'ab; cd;ef';'it is ''aight';;234;';;';'\\;'",
            UTF_8
        ));
        assertThat(splits).containsExactly(
            "'foo 1'",
            "bar",
            "234",
            "'ab; cd;ef'",
            "'it is ''aight'",
            "",
            "234",
            "';;'",
            "'\\;'"
        );
    }

    @Test
    void quotedPickAll() {
        List<String> splits = new ArrayList<>();
        String input = "'foo 1';bar;234;'ab; cd;ef';'it is ''aight';;234;';';'\\;'";
        Consumer<LineSegment> splitter = LineSplitters.csvSink(DQ_FORMAT, adder(splits));
        splitter.accept(LineSegments.of(input, UTF_8));
        assertThat(splits).containsExactly(
            "'foo 1'",
            "bar",
            "234",
            "'ab; cd;ef'",
            "'it is ''aight'",
            "",
            "234",
            "';'",
            "'\\;'"
        );
    }

    @Test
    void splitFile() {
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
    void quotesFiles() {
        assertFileContents(
            """
                foo;'123; ok'
                'b; ar';42
                """,
            "foo",
            "'123; ok'",
            "'b; ar'",
            "42"

        );
    }

    @Test
    void trickyFile() {
        assertFileContents(
            """
                '';
                'f;o;o';bar;zot
                123123123;234234234;345345345
                '1;2';3
                ;
                """,
            "''",
            "",
            "'f;o;o'",
            "bar",
            "zot",
            "123123123",
            "234234234",
            "345345345",
            "'1;2'",
            "3",
            "",
            ""
        );
    }

    @Test
    void trickyFile2() {
        assertFileContents(
            """
                '';
                'f;o;o'
                """,
            "''",
            "",
            "'f;o;o'"
        );
    }

    private Partitioned partitioned(Partitioning partitioning, String contents) {
        try {
            return PartitionedPaths.partitioned(fileWith(contents), partitioning);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Path fileWith(String contents) {
        try {
            return Files.writeString(tempDir.resolve(UUID.randomUUID() + ".txt"), contents, CREATE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assertSplit(Partitioning partitioning, String input) {
        List<String> exp = Arrays.stream(input.split("\n")).flatMap(line -> Arrays.stream(line.split(";")))
            .toList();

        List<String> splits = splits(partitioning, input, DQ_FORMAT);
        assertThat(splits).containsExactlyElementsOf(exp);
    }

    private void assertSplit(
        Partitioning partitioning, String input, String... expected
    ) {
        assertSplit(partitioning, input, DQ_FORMAT, expected);
    }

    @SuppressWarnings("SameParameterValue")
    private void assertSplit(
        Partitioning partitioning, String input, Format.Csv format, String... expected
    ) {
        assertThat(splits(partitioning, input, format)).containsExactly(expected);
    }

    private List<String> splits(Partitioning partitioning, String input, Format.Csv format) {
        List<String> splits = new ArrayList<>();
        try {
            try (Partitioned partitioned = partitioned(partitioning, input.trim() + "\n")) {
                partitioned.splitters(format)
                    .forEach(consumer -> consumer.forEach(commaSeparatedLine -> commaSeparatedLine.columns(UTF_8)
                        .forEach(splits::add)));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed: " + String.join("\n", splits), e);
        }
        return splits;
    }

    public static final Format.Csv.Quoted DQ_FORMAT = Formats.Csv.quoted(';', '\'');

    private static void assertFileContents(String contents, String... lines) {
        List<String> splits = new ArrayList<>();
        Path path;
        try {
            path = Files.write(
                Files.createTempFile(UUID.randomUUID().toString(), ".txt"),
                List.of(contents.split("\n"))
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        BitwiseCsvQuotedSplitter splitter = new BitwiseCsvQuotedSplitter(
            line -> line.columns(UTF_8)
                .forEach(splits::add), DQ_FORMAT
        );

        try {
            try (Partitioned partititioned = PartitionedPaths.partitioned(path, Partitioning.single())) {
                partititioned.streamers()
                    .forEach(streamer -> streamer.lines()
                        .forEach(splitter));
            }
            if (lines.length > 0) {
                assertThat(splits).containsExactly(lines);
            } else {
                assertThat(splits).containsExactlyElementsOf(Arrays.stream(contents.split("\n"))
                    .flatMap(s -> Arrays.stream(s.split(";")))
                    .toList());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed with list: " + String.join("\n  ", splits), e);
        }
    }

    private static Consumer<SeparatedLine> adder(List<String> splits) {
        return line -> line.columns(UTF_8)
            .forEach(splits::add);
    }
}
