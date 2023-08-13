package com.github.kjetilv.lopp.lc;

import com.github.kjetilv.lopp.PartitionBytes;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public interface LineCounter {

    default long count(Path path) {
        return scan(path).linesCount();
    }

    default Lines scan(Path path) {
        return new Lines() {

            @Override
            public long linesCount() {
                return count(path);
            }

            @Override
            public int longestLine() {
                return 0;
            }

            @Override
            public List<PartitionBytes> bytesPartitions() {
                return Collections.emptyList();
            }
        };
    }

    interface Lines {

        long linesCount();

        int longestLine();

        List<PartitionBytes> bytesPartitions();
    }

    record LineOffset(long bytePosition, long lineNo) {
    }
}
