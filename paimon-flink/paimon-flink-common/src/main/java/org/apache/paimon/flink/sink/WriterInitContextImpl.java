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

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** WriterInitContextImpl. */
public class WriterInitContextImpl implements WriterInitContext {

    private final OptionalLong restoredCheckpointId;

    private final StreamingRuntimeContext runtimeContext;

    private final org.apache.flink.streaming.runtime.tasks.ProcessingTimeService
            processingTimeService;

    private final MailboxExecutor mailboxExecutor;

    private final SinkWriterMetricGroup metricGroup;

    private final StreamConfig operatorConfig;

    private Consumer<?> metaConsumer;

    public WriterInitContextImpl(
            StreamingRuntimeContext runtimeContext,
            org.apache.flink.streaming.runtime.tasks.ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor,
            SinkWriterMetricGroup metricGroup,
            StreamConfig operatorConfig,
            OptionalLong restoredCheckpointId) {
        this.runtimeContext = checkNotNull(runtimeContext);
        this.restoredCheckpointId = restoredCheckpointId;
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.processingTimeService = checkNotNull(processingTimeService);
        this.metricGroup = checkNotNull(metricGroup);
        this.operatorConfig = checkNotNull(operatorConfig);
    }

    public <MetaT> void withMetaConsumer(Consumer<MetaT> metaConsumer) {
        this.metaConsumer = metaConsumer;
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return restoredCheckpointId;
    }

    @Override
    public JobInfo getJobInfo() {
        return runtimeContext.getJobInfo();
    }

    @Override
    public TaskInfo getTaskInfo() {
        return runtimeContext.getTaskInfo();
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return new UserCodeClassLoader() {
            @Override
            public ClassLoader asClassLoader() {
                return runtimeContext.getUserCodeClassLoader();
            }

            @Override
            public void registerReleaseHookIfAbsent(String releaseHookName, Runnable releaseHook) {
                runtimeContext.registerUserCodeClassLoaderReleaseHookIfAbsent(
                        releaseHookName, releaseHook);
            }
        };
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        return mailboxExecutor;
    }

    @Override
    public org.apache.flink.api.common.operators.ProcessingTimeService getProcessingTimeService() {
        return processingTimeService;
    }

    @Override
    public SinkWriterMetricGroup metricGroup() {
        return metricGroup;
    }

    @Override
    public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
        return new InitContextInitializationContextAdapter(
                getUserCodeClassLoader(), () -> metricGroup.addGroup("user"));
    }

    @Override
    public boolean isObjectReuseEnabled() {
        return runtimeContext.isObjectReuseEnabled();
    }

    @Override
    public <IN> TypeSerializer<IN> createInputSerializer() {
        return operatorConfig
                .<IN>getTypeSerializerIn(0, runtimeContext.getUserCodeClassLoader())
                .duplicate();
    }

    @Override
    public <MetaT> Optional<Consumer<MetaT>> metadataConsumer() {
        return Optional.ofNullable((Consumer<MetaT>) metaConsumer);
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

    /**
     * A callback interface that the user can implement to know the offset of the bucket when the
     * request is complete.
     */
    public interface WriteCallback {

        /**
         * A callback method the user can implement to provide asynchronous handling of request
         * completion. This method will be called when the record sent to the server has been
         * acknowledged.
         */
        void onCompletion(int bucket, long offset);
    }
}
