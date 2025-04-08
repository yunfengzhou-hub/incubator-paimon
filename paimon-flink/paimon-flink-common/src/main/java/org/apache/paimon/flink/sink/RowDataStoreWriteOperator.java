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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/** A {@link PrepareCommitOperator} to write {@link InternalRow}. Record schema is fixed. */
public class RowDataStoreWriteOperator extends TableWriteOperator<InternalRow> {

    private static final long serialVersionUID = 3L;

    protected RowDataStoreWriteOperator(
            StreamOperatorParameters<Committable> parameters,
            FileStoreTable table,
            StoreSinkWrite.Provider storeSinkWriteProvider,
            String initialCommitUser) {
        super(parameters, table, storeSinkWriteProvider, initialCommitUser);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
    }

    @Override
    protected boolean containLogSystem() {
        return false;
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
    }

    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {

        SinkRecord record;
        try {
            record = write.write(element.getValue());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
    }

    @Override
    public void finish() throws Exception {
        super.finish();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
    }

    @Override
    protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<Committable> committables = super.prepareCommit(waitCompaction, checkpointId);

        return committables;
    }

    /** {@link StreamOperatorFactory} of {@link RowDataStoreWriteOperator}. */
    public static class Factory extends TableWriteOperator.Factory<InternalRow> {

        public Factory(
                FileStoreTable table,
                StoreSinkWrite.Provider storeSinkWriteProvider,
                String initialCommitUser) {
            super(table, storeSinkWriteProvider, initialCommitUser);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Committable>> T createStreamOperator(
                StreamOperatorParameters<Committable> parameters) {
            return (T)
                    new RowDataStoreWriteOperator(
                            parameters, table, storeSinkWriteProvider, initialCommitUser);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return RowDataStoreWriteOperator.class;
        }
    }

    private static class InitContextInitializationContextAdapter
            implements SerializationSchema.InitializationContext {

        private final UserCodeClassLoader userCodeClassLoader;
        private final Supplier<MetricGroup> metricGroupSupplier;
        private MetricGroup cachedMetricGroup;

        public InitContextInitializationContextAdapter(
                UserCodeClassLoader userCodeClassLoader,
                Supplier<MetricGroup> metricGroupSupplier) {
            this.userCodeClassLoader = userCodeClassLoader;
            this.metricGroupSupplier = metricGroupSupplier;
        }

        @Override
        public MetricGroup getMetricGroup() {
            if (cachedMetricGroup == null) {
                cachedMetricGroup = metricGroupSupplier.get();
            }
            return cachedMetricGroup;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return userCodeClassLoader;
        }
    }
}
