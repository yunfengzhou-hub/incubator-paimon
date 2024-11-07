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

package org.apache.paimon.flink.action;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.table.Table;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.data.RowData;

import java.util.Map;

/** Abstract base of {@link Action} for table. */
public abstract class TableActionBase extends ActionBase {

    protected Table table;
    protected final Identifier identifier;

    TableActionBase(
            String warehouse,
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig) {
        super(warehouse, catalogConfig);
        identifier = new Identifier(databaseName, tableName);
        try {
            table = catalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    /** Sink {@link DataStream} dataStream to table. */
    public JobClient batchSink(DataStream<RowData> dataStream) throws Exception {
        new FlinkSinkBuilder(table).forRowData(dataStream).build().name(identifier.getFullName());
        return ((StreamTableEnvironmentImpl) batchTEnv).execEnv().executeAsync();
    }
}
