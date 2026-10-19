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

package org.apache.paimon.flink.lakestream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Characterization test that empirically records the order in which the Flink planner invokes the
 * {@link org.apache.flink.table.connector.sink.DynamicTableSink} callbacks ({@code applyOverwrite},
 * {@code applyStaticPartition}, {@code getChangelogMode}, {@code getSinkRuntimeProvider}, {@code
 * copy}, {@code asSummaryString}) for plain {@code INSERT INTO} and {@code INSERT OVERWRITE}
 * statements.
 *
 * <p>This pins down the assumption behind the PIP-39 routing sink ({@link
 * LakeStreamFlinkTableSink}): the planner creates the sink before it knows whether the statement is
 * an overwrite, and delivers that only through {@code applyOverwrite(boolean)}. So the sink has to
 * branch on a flag that arrives after construction, which is only sound if {@code applyOverwrite}
 * precedes every callback that reads the flag. If that assumption were false, these tests would
 * fail and the routing sink would have to be redesigned.
 *
 * <p>The callbacks that read the flag and shape the executed plan are {@code getChangelogMode},
 * {@code getSinkRuntimeProvider}, {@code applyStaticPartition} and {@code
 * requiresPartitionGrouping}, and each is pinned below. {@code asSummaryString} reads it too, but
 * only names the sink in plan explanations, so its position is not load bearing.
 */
public class SinkCallbackOrderITCase {

    @BeforeEach
    public void beforeEach() {
        RecordingSinkFactory.reset();
    }

    private StreamTableEnvironment newTableEnv(RuntimeExecutionMode mode) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setRuntimeMode(mode);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE recording_sink (id INT) "
                        + "WITH ('connector' = 'recording-sink')");
        return tEnv;
    }

    private static int firstIndexOf(List<String> calls, String callback) {
        String needle = "." + callback;
        for (int i = 0; i < calls.size(); i++) {
            if (calls.get(i).contains(needle)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The PIP-39 gate: {@code getChangelogMode} must never be observed before {@code
     * applyOverwrite(true)}, because the routing sink's {@code getChangelogMode()} is served by
     * whichever side the overwrite flag selects. Two benign cases are fine and observed in
     * practice:
     *
     * <ul>
     *   <li>{@code applyOverwrite} is never called (plain {@code INSERT INTO}: the planner skips
     *       it, so the routing sink keeps its default {@code overwrite == false} and correctly
     *       serves the changelog-mode queries from the store sink);
     *   <li>{@code getChangelogMode} is never called (batch mode does no changelog inference).
     * </ul>
     */
    private static void assertOverwriteAppliedBeforeChangelogMode(List<String> calls) {
        int overwriteTrueIdx = firstIndexOf(calls, "applyOverwrite(true)");
        int changelogIdx = firstIndexOf(calls, "getChangelogMode");
        boolean changelogQueriedBeforeOverwriteApplied =
                changelogIdx >= 0 && overwriteTrueIdx >= 0 && changelogIdx < overwriteTrueIdx;
        assertThat(changelogQueriedBeforeOverwriteApplied)
                .describedAs(
                        "getChangelogMode must NOT be invoked before applyOverwrite(true); "
                                + "observed order: %s",
                        calls)
                .isFalse();
    }

    @Test
    public void testStreamingInsertIntoCallbackOrder() throws Exception {
        StreamTableEnvironment tEnv = newTableEnv(RuntimeExecutionMode.STREAMING);

        tEnv.executeSql("INSERT INTO recording_sink SELECT 1").await();

        List<String> calls = RecordingSinkFactory.calls();
        System.out.println("OBSERVED-CALLBACK-ORDER streaming INSERT INTO: " + calls);

        // the planner skips applyOverwrite entirely for a plain INSERT INTO
        // (validateAndApplyOverwrite
        // returns early when overwrite == false), so the routing sink's default (store sink) is
        // what serves the changelog-mode queries
        assertThat(firstIndexOf(calls, "applyOverwrite(true)")).isEqualTo(-1);
        assertThat(firstIndexOf(calls, "getChangelogMode")).isNotNegative();
        assertThat(firstIndexOf(calls, "getChangelogMode"))
                .isLessThan(firstIndexOf(calls, "getSinkRuntimeProvider"));
    }

    @Test
    public void testBatchInsertIntoCallbackOrder() throws Exception {
        StreamTableEnvironment tEnv = newTableEnv(RuntimeExecutionMode.BATCH);

        tEnv.executeSql("INSERT INTO recording_sink SELECT 1").await();

        List<String> calls = RecordingSinkFactory.calls();
        System.out.println("OBSERVED-CALLBACK-ORDER batch INSERT INTO: " + calls);

        assertThat(firstIndexOf(calls, "applyOverwrite(true)")).isEqualTo(-1);
        assertThat(firstIndexOf(calls, "getSinkRuntimeProvider")).isNotNegative();
    }

    @Test
    public void testBatchInsertOverwriteCallbackOrder() throws Exception {
        StreamTableEnvironment tEnv = newTableEnv(RuntimeExecutionMode.BATCH);

        tEnv.executeSql("INSERT OVERWRITE recording_sink SELECT 1").await();

        List<String> calls = RecordingSinkFactory.calls();
        System.out.println("OBSERVED-CALLBACK-ORDER batch INSERT OVERWRITE: " + calls);

        assertThat(calls).anyMatch(c -> c.contains(".applyOverwrite(true)"));
        assertThat(firstIndexOf(calls, "applyOverwrite"))
                .isLessThan(firstIndexOf(calls, "getSinkRuntimeProvider"));
        assertOverwriteAppliedBeforeChangelogMode(calls);
    }

    @Test
    public void testBatchInsertOverwriteWithStaticPartitionCallbackOrder() throws Exception {
        StreamTableEnvironment tEnv = newTableEnv(RuntimeExecutionMode.BATCH);
        createPartitionedRecordingSink(tEnv);

        tEnv.executeSql("INSERT OVERWRITE recording_sink_part PARTITION (dt = '20261019') SELECT 1")
                .await();

        List<String> calls = RecordingSinkFactory.calls();
        System.out.println(
                "OBSERVED-CALLBACK-ORDER batch INSERT OVERWRITE with a static partition: " + calls);

        // the routing sink forwards the static partition to whichever side the flag selects, so the
        // flag has to arrive first or the partition would be applied to the store and the overwrite
        // executed by Paimon
        assertThat(firstIndexOf(calls, "applyStaticPartition")).isNotNegative();
        assertThat(firstIndexOf(calls, "applyOverwrite(true)"))
                .isLessThan(firstIndexOf(calls, "applyStaticPartition"))
                .isLessThan(firstIndexOf(calls, "getSinkRuntimeProvider"));
    }

    @Test
    public void testBatchInsertOverwriteWithDynamicPartitionCallbackOrder() throws Exception {
        StreamTableEnvironment tEnv = newTableEnv(RuntimeExecutionMode.BATCH);
        createPartitionedRecordingSink(tEnv);

        // the partition column stays dynamic: the planner only asks about partition grouping when
        // some partition field is not pinned by the statement
        tEnv.executeSql("INSERT OVERWRITE recording_sink_part SELECT 1, '20261019'").await();

        List<String> calls = RecordingSinkFactory.calls();
        System.out.println(
                "OBSERVED-CALLBACK-ORDER batch INSERT OVERWRITE with a dynamic partition: "
                        + calls);

        // the routing sink also reads the flag from requiresPartitionGrouping, which the physical
        // sink rule calls, so the flag has to be applied before optimization starts
        assertThat(firstIndexOf(calls, "requiresPartitionGrouping")).isNotNegative();
        assertThat(firstIndexOf(calls, "applyOverwrite(true)"))
                .isLessThan(firstIndexOf(calls, "requiresPartitionGrouping"))
                .isLessThan(firstIndexOf(calls, "getSinkRuntimeProvider"));
    }

    private static void createPartitionedRecordingSink(StreamTableEnvironment tEnv) {
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE recording_sink_part (id INT, dt STRING) "
                        + "PARTITIONED BY (dt) WITH ('connector' = 'recording-sink')");
    }

    @Test
    public void testStreamingInsertOverwriteCallbackOrder() throws Exception {
        StreamTableEnvironment tEnv = newTableEnv(RuntimeExecutionMode.STREAMING);

        Throwable failure = null;
        try {
            tEnv.executeSql("INSERT OVERWRITE recording_sink SELECT 1").await();
        } catch (Throwable t) {
            failure = t;
        }

        List<String> calls = RecordingSinkFactory.calls();
        System.out.println(
                "OBSERVED-CALLBACK-ORDER streaming INSERT OVERWRITE: "
                        + calls
                        + (failure == null ? "" : " | failure: " + failure));

        // either the planner rejects streaming INSERT OVERWRITE, or it applies the overwrite flag
        // before querying the changelog mode - both are safe for the routing sink design
        if (failure == null) {
            assertThat(calls).anyMatch(c -> c.contains(".applyOverwrite(true)"));
        }
        assertOverwriteAppliedBeforeChangelogMode(calls);
    }
}
