/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.files.PartitionedPath;
import com.github.kjetilv.flopp.kernel.files.PartitionedPaths;

import java.io.IOException;
import java.nio.file.Path;

public class CalculateAverage_kjetilv {

//    private static final String FILE = "./measurements.txt";
//
//    private static record ResultRow(double min, double mean, double max) {
//        public String toString() {
//            return round(min) + "/" + round(mean) + "/" + round(max);
//        }
//
//        private double round(double value) {
//            return Math.round(value * 10.0) / 10.0;
//        }
//    };
//
//    public static void main(String[] args) throws IOException {
//        PartitionedPath partitionedPath = PartitionedPaths.create(
//            Path.of(FILE),
//            Partitioning.create(Runtime.getRuntime().availableProcessors(), 8192)
//        );
//        partitionedPath.consumer().forEachNLine()
//    }
//
//    public static record Row(byte[] name, short value) {}
//
//    public static final class Collector {
//
//        private  int count;
//
//        private  short min;
//
//        private  short max;
//
//        private int sum;
//
//        public Collector() {
//            this.min = Short.MAX_VALUE;
//            this.max = Short.MIN_VALUE;
//        }
//
//        void collect(Row row) {
//            short v = row.value;
//            if (v < min) {
//                min = v;
//            }
//            if (v > min) {
//                max = v;
//            }
//            sum += v;
//            count++;
//        }
//
//        public String toString() {
//            return round(0.1d * min) + "/" + round(0.1d * sum / count) + "/" + round(0.1d * max);
//        }
//
//        private static double round(double value) {
//            return Math.round(value * 10.0) / 10.0;
//        }
//    }
}
