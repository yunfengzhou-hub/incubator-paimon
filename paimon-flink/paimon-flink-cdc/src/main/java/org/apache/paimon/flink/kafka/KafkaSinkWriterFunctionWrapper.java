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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.flink.sink.WriterInitContextImpl;
import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.runtime.operators.sink.SinkWriterFunctionWrapper;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.function.Consumer;

/** KafkaSinkWriterFunctionWrapper. */
public class KafkaSinkWriterFunctionWrapper extends SinkWriterFunctionWrapper {
    protected KafkaSinkWriterFunctionWrapper(Sink<SinkRecord> sink) {
        super(sink);
    }

    @Override
    public void setWriteCallback(WriteCallback writeCallback) {
        Consumer<RecordMetadata> consumer =
                metadata -> {
                    if (writeCallback != null && metadata != null && metadata.hasOffset()) {
                        writeCallback.onCompletion(metadata.partition(), metadata.offset());
                    }
                };
        ((WriterInitContextImpl) writerInitContext).withMetaConsumer(consumer);
    }
}
