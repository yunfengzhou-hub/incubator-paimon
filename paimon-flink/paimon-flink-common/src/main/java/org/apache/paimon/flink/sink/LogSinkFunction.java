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

import org.apache.paimon.table.sink.SinkRecord;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;

/** Log {@link SinkFunction} with {@link WriteCallback}. */
public interface LogSinkFunction extends Function {

    /**
     * Writes the given value to the sink. This function is called for every record.
     *
     * <p>You have to override this method when implementing a {@code SinkFunction}, this is a
     * {@code default} method for backward compatibility with the old-style method only.
     *
     * @param value The input record.
     * @param context Additional context about the input record.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    void invoke(SinkRecord value, Context context) throws Exception;

    /**
     * Writes the given watermark to the sink. This function is called for every watermark.
     *
     * <p>This method is intended for advanced sinks that propagate watermarks.
     *
     * @param watermark The watermark.
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    default void writeWatermark(Watermark watermark) throws Exception {}

    /**
     * This method is called at the end of data processing.
     *
     * <p>The method is expected to flush all remaining buffered data. Exceptions will cause the
     * pipeline to be recognized as failed, because the last data items are not processed properly.
     * You may use this method to flush remaining buffered elements in the state into transactions
     * which you can commit in the last checkpoint.
     *
     * <p><b>NOTE:</b>This method does not need to close any resources. You should release external
     * resources in the {@link RichSinkFunction#close()} method.
     *
     * @throws Exception This method may throw exceptions. Throwing an exception will cause the
     *     operation to fail and may trigger recovery.
     */
    default void finish() throws Exception {}

    default void setWriterInitContext(WriterInitContext writerInitContext) {}

    void setWriteCallback(WriteCallback writeCallback);

    /** Flush pending records. */
    void flush() throws Exception;

    /** Context. */
    interface Context extends SinkWriter.Context {
        long currentProcessingTime();
    }

    /**
     * A callback interface that the user can implement to know the offset of the bucket when the
     * request is complete.
     */
    interface WriteCallback {

        /**
         * A callback method the user can implement to provide asynchronous handling of request
         * completion. This method will be called when the record sent to the server has been
         * acknowledged.
         */
        void onCompletion(int bucket, long offset);
    }
}
