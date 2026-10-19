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

import org.apache.flink.api.connector.sink2.InitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A test-only {@link DynamicTableSinkFactory} whose sink records the exact order in which the Flink
 * planner invokes the {@link DynamicTableSink} callbacks. It is registered under the identifier
 * {@code recording-sink} in the test-scope {@code
 * META-INF/services/org.apache.flink.table.factories.Factory} and used by {@link
 * SinkCallbackOrderITCase} to pin down the planner's real callback ordering (the PIP-39 routing
 * sink design assumes {@code applyOverwrite} happens before {@code getChangelogMode}).
 */
public class RecordingSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "recording-sink";

    private static final List<String> CALLS = Collections.synchronizedList(new ArrayList<String>());
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();

    /** The recorded callback order, e.g. {@code [s1.applyOverwrite(false), ...]}. */
    public static List<String> calls() {
        synchronized (CALLS) {
            return new ArrayList<String>(CALLS);
        }
    }

    public static void reset() {
        CALLS.clear();
        INSTANCE_COUNTER.set(0);
    }

    private static void record(String event) {
        CALLS.add(event);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        record("createDynamicTableSink");
        return new RecordingSink("s" + INSTANCE_COUNTER.incrementAndGet());
    }

    /** The recording {@link DynamicTableSink}. Writes are discarded by a no-op runtime sink. */
    static class RecordingSink
            implements DynamicTableSink, SupportsOverwrite, SupportsPartitioning {

        private final String id;

        RecordingSink(String id) {
            this.id = id;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            record(id + ".getChangelogMode");
            return requestedMode;
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            record(id + ".getSinkRuntimeProvider");
            return SinkV2Provider.of(new NoOpSink());
        }

        @Override
        public DynamicTableSink copy() {
            record(id + ".copy");
            return new RecordingSink(id + "c");
        }

        @Override
        public String asSummaryString() {
            record(id + ".asSummaryString");
            return "RecordingSink(" + id + ")";
        }

        @Override
        public void applyOverwrite(boolean overwrite) {
            record(id + ".applyOverwrite(" + overwrite + ")");
        }

        @Override
        public void applyStaticPartition(Map<String, String> staticPartition) {
            record(id + ".applyStaticPartition");
        }

        @Override
        public boolean requiresPartitionGrouping(boolean isMultiSink) {
            record(id + ".requiresPartitionGrouping");
            return false;
        }
    }

    static class NoOpSink implements Sink<RowData> {
        /**
         * Do not annotate with <code>@override</code> here to maintain compatibility with Flink
         * 2.0+.
         */
        public SinkWriter<RowData> createWriter(InitContext context) {
            return new NoOpWriter();
        }

        /**
         * Do not annotate with <code>@override</code> here to maintain compatibility with Flink
         * 1.18-.
         */
        public SinkWriter<RowData> createWriter(WriterInitContext context) {
            return new NoOpWriter();
        }
    }

    static class NoOpWriter implements SinkWriter<RowData> {
        @Override
        public void write(RowData element, Context context) {}

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public void close() {}
    }
}
