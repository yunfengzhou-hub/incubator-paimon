/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.benchmark;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class MyBenchmarkTest extends TableBenchmark {
    @ParameterizedTest
    @MethodSource("paramsProvider")
    public void test(TestParams params) throws Exception {
        long valuesPerIteration = 300_000;
        int numCommit = params.numCommit;

        System.out.printf(
                "running benchmark with No. commit: %s\n",
                numCommit);
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FileFormatType.ORC);
        options.set("orc.compress", "none");
        innerTest(
                CoreOptions.FileFormatType.ORC.name(),
                options,
                valuesPerIteration,
                valuesPerIteration / numCommit);
    }

    static Stream<TestParams> paramsProvider() {
        List<Integer> numCommits = Arrays.asList(1,2,3,4,5,6);
        List<TestParams> list = new ArrayList<>();
        for (int numCommit: numCommits) {
            TestParams params = new TestParams();
            params.numCommit = numCommit;
            list.add(params);
        }
        return list.stream();
    }

    static class TestParams {
        int numCommit;
    }

    public void innerTest(
            String name, Options options, long valuesPerIteration, long valuesPerCommit)
            throws Exception {
        StreamWriteBuilder writeBuilder = createTable(options).newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();
        StreamTableCommit commit = writeBuilder.newCommit();
        Benchmark benchmark =
                new Benchmark(name, valuesPerIteration)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);
        AtomicInteger writeCount = new AtomicInteger(0);
        AtomicInteger commitIdentifier = new AtomicInteger(0);
        benchmark.addCase(
                "write",
                5,
                () -> {
                    for (int i = 0; i < valuesPerIteration; i++) {
                        try {
                            write.write(newRandomRow());
                            writeCount.incrementAndGet();
                            if (writeCount.get() % valuesPerCommit == 0) {
                                List<CommitMessage> commitMessages =
                                        write.prepareCommit(false, commitIdentifier.get());
                                commit.commit(commitIdentifier.get(), commitMessages);
                                commitIdentifier.incrementAndGet();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
        benchmark.run();
        write.close();
    }
}
