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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.operation.ListUnexistingFilesTest;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link RemoveUnexistingFilesProcedure}. */
public class RemoveUnexistingFilesProcedureITCase extends AbstractTestBase {

    @ParameterizedTest
    @ValueSource(ints = {-1, 3})
    public void testProcedure(int bucket) throws Exception {
        String warehouse = getTempDirPath();
        int numPartitions = 2;
        int numFiles = 10;
        int[] numDeletes = new int[numPartitions];
        ListUnexistingFilesTest.prepareRandomlyDeletedTable(
                warehouse, "mydb", "t", bucket, numFiles, numDeletes);

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql(
                "CREATE CATALOG mycat WITH (\n"
                        + "  'type' = 'paimon',\n"
                        + "  'warehouse' = '"
                        + warehouse
                        + "'\n"
                        + ")");
        tEnv.executeSql("USE CATALOG mycat");

        int[] actual = new int[numPartitions];
        Pattern pattern = Pattern.compile("pt=(\\d+?)/");
        try (CloseableIterator<Row> it =
                tEnv.executeSql(
                                "CALL sys.remove_unexisting_files(`table` => 'mydb.t', `dry_run` => true, `parallelism` => 2)")
                        .collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                Matcher matcher = pattern.matcher(row.getField(0).toString());
                if (matcher.find()) {
                    actual[Integer.parseInt(matcher.group(1))]++;
                }
            }
        }
        assertThat(actual).isEqualTo(numDeletes);

        tEnv.executeSql("CALL sys.remove_unexisting_files(`table` => 'mydb.t', `parallelism` => 2)")
                .await();
        try (CloseableIterator<Row> it =
                tEnv.executeSql("SELECT pt, CAST(COUNT(*) AS INT) FROM mydb.t GROUP BY pt")
                        .collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                assertThat(row.getField(1)).isEqualTo(numFiles - numDeletes[(int) row.getField(0)]);
            }
        }
    }
}
