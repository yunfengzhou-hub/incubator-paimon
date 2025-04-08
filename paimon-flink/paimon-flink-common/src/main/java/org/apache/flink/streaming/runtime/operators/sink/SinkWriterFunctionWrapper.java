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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.paimon.flink.sink.LogSinkFunction;
import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/** SinkWriterFunctionWrapper. */
public abstract class SinkWriterFunctionWrapper
        implements LogSinkFunction, RichFunction, CheckpointedFunction {
    protected final Sink<SinkRecord> sink;
    protected transient SinkWriterStateHandler<SinkRecord> writerStateHandler;
    protected transient WriterInitContext writerInitContext;
    protected transient StateInitializationContext stateInitializationContext;
    protected transient SinkWriter<SinkRecord> sinkWriter;
    private transient RuntimeContext runtimeContext;

    protected SinkWriterFunctionWrapper(Sink<SinkRecord> sink) {
        this.sink = sink;
    }

    @Override
    public void invoke(SinkRecord value, Context context) throws Exception {
        sinkWriter.write(value, context);
    }

    @Override
    public void writeWatermark(Watermark watermark) throws Exception {
        sinkWriter.writeWatermark(watermark);
    }

    @Override
    public void flush() throws Exception {
        sinkWriter.flush(false);
    }

    @Override
    public void finish() throws Exception {
        sinkWriter.flush(true);
    }

    public void open(Configuration configuration) throws Exception {
        open(new OpenContext() {});
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        if (sink instanceof SupportsWriterState) {
            writerStateHandler =
                    new StatefulSinkWriterStateHandler<>((SupportsWriterState<SinkRecord, ?>) sink);
        } else {
            writerStateHandler = new StatelessSinkWriterStateHandler<>(sink);
        }
        sinkWriter = writerStateHandler.createWriter(writerInitContext, stateInitializationContext);
    }

    @Override
    public void close() throws Exception {
        sinkWriter.close();
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        if (this.runtimeContext != null) {
            return this.runtimeContext;
        } else {
            throw new IllegalStateException("The runtime context has not been initialized.");
        }
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        if (this.runtimeContext == null) {
            throw new IllegalStateException("The runtime context has not been initialized.");
        } else if (this.runtimeContext instanceof IterationRuntimeContext) {
            return (IterationRuntimeContext) this.runtimeContext;
        } else {
            throw new IllegalStateException("This stub is not part of an iteration step function.");
        }
    }

    @Override
    public void setRuntimeContext(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        writerStateHandler.snapshotState(functionSnapshotContext.getCheckpointId());
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext)
            throws Exception {
        this.stateInitializationContext =
                (StateInitializationContext) functionInitializationContext;
    }

    public void setWriterInitContext(WriterInitContext writerInitContext) {
        this.writerInitContext = writerInitContext;
    }
}
