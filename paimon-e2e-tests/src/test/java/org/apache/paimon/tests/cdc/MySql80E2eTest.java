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

package org.apache.paimon.tests.cdc;

import org.apache.paimon.flink.action.cdc.mysql.MySqlVersion;

import org.junit.jupiter.api.condition.EnabledIf;

/**
 * E2e tests for {@link org.apache.paimon.flink.action.cdc.mysql.MySqlSyncTableAction} with MySQL
 * 8.0.
 */
@EnabledIf("runTest")
public class MySql80E2eTest extends MySqlCdcE2eTestBase {

    private static boolean runTest() {
        // TODO: modify the following condition after paimon-flink-cdc supports flink 2.0
        String flinkVersion = System.getProperty("test.flink.main.version");
        return flinkVersion.compareTo("2.0") < 0;
    }

    public MySql80E2eTest() {
        super(MySqlVersion.V8_0);
    }
}
