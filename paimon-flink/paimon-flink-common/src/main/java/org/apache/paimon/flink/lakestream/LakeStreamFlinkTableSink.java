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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.sink.FlinkTableSink;
import org.apache.paimon.table.Table;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The sink of a lake-stream table (PIP-39 L88-96, L283-350): regular writes are executed by the
 * Lakestream Store, {@code INSERT OVERWRITE} is executed by Paimon.
 *
 * <p>This is a {@link FlinkTableSink} rather than a wrapper around one. The planner creates the
 * sink before it knows whether the statement is an overwrite, and only tells the sink afterwards
 * through {@code applyOverwrite(boolean)}, so a single object has to serve both. Extending Paimon's
 * sink reuses the {@code overwrite} flag Paimon already keeps for exactly this purpose, and
 * inherits every sink ability Paimon's sink has instead of re-declaring a subset of them: a wrapper
 * that listed the abilities by hand would hide the ones it forgot, and the planner discovers
 * abilities with {@code instanceof}.
 *
 * <p>Every ability therefore has to be answered by whichever side executes the statement, which is
 * why each inherited {@code apply*} is overridden here. The one exception is {@code applyOverwrite}
 * itself: the inherited implementation sets exactly the flag this class branches on, and the flag
 * is deliberately not forwarded to the store, because an overwrite is executed by Paimon and a
 * store that validates the flag would otherwise reject a statement it never runs. {@code
 * LakeStreamFlinkTableSinkAbilityCoverageTest} fails the build if Paimon's sink grows an ability
 * that this class neither handles nor exempts for a stated reason.
 *
 * <p>The inherited abilities are Paimon's, not the store's. When the store's sink lacks one that a
 * statement needs, the failure surfaces when the planner applies that ability, naming the store and
 * the missing ability, rather than when this sink is created.
 *
 * <p>{@code TRUNCATE TABLE} is rejected: it would clear the Paimon table and leave the store's copy
 * of the same data behind.
 */
public class LakeStreamFlinkTableSink extends FlinkTableSink {

    private final DynamicTableSink storeSink;

    public LakeStreamFlinkTableSink(
            ObjectIdentifier tableIdentifier,
            Table table,
            DynamicTableFactory.Context context,
            DynamicTableSink storeSink) {
        super(tableIdentifier, table, context);
        this.storeSink = storeSink;
    }

    private <T> T storeAs(Class<T> ability) {
        if (!ability.isInstance(storeSink)) {
            throw new ValidationException(
                    String.format(
                            "Table '%s' is a lake-stream table, so this operation is executed by "
                                    + "the Lakestream Store, but the store's sink %s does not "
                                    + "implement %s.",
                            tableIdentifier.asSummaryString(),
                            storeSink.getClass().getName(),
                            ability.getSimpleName()));
        }
        return ability.cast(storeSink);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return overwrite
                ? super.getChangelogMode(requestedMode)
                : storeSink.getChangelogMode(requestedMode);
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return overwrite
                ? super.getSinkRuntimeProvider(context)
                : storeSink.getSinkRuntimeProvider(context);
    }

    @Override
    public String asSummaryString() {
        return overwrite ? super.asSummaryString() : storeSink.asSummaryString();
    }

    /**
     * {@link org.apache.paimon.flink.sink.SupportsRowLevelOperationFlinkTableSink#copy()} hardcodes
     * {@code new FlinkTableSink(...)}, so without this override a copy would silently lose the
     * routing and write to Paimon.
     */
    @Override
    public DynamicTableSink copy() {
        LakeStreamFlinkTableSink copied =
                new LakeStreamFlinkTableSink(
                        tableIdentifier, table, this.context, storeSink.copy());
        copied.staticPartitions = new HashMap<>(staticPartitions);
        copied.overwrite = overwrite;
        return copied;
    }

    @Override
    public void applyStaticPartition(Map<String, String> staticPartition) {
        // the planner applies the overwrite flag first, so the executing side is already known
        // here; SinkCallbackOrderITCase pins that order
        if (overwrite) {
            super.applyStaticPartition(staticPartition);
        } else {
            storeAs(SupportsPartitioning.class).applyStaticPartition(staticPartition);
        }
    }

    @Override
    public boolean requiresPartitionGrouping(boolean isMultiSink) {
        return overwrite
                ? super.requiresPartitionGrouping(isMultiSink)
                : storeAs(SupportsPartitioning.class).requiresPartitionGrouping(isMultiSink);
    }

    @Override
    public RowLevelUpdateInfo applyRowLevelUpdate(
            List<Column> updatedColumns, @Nullable RowLevelModificationScanContext context) {
        return storeAs(SupportsRowLevelUpdate.class).applyRowLevelUpdate(updatedColumns, context);
    }

    @Override
    public RowLevelDeleteInfo applyRowLevelDelete(
            @Nullable RowLevelModificationScanContext context) {
        return storeAs(SupportsRowLevelDelete.class).applyRowLevelDelete(context);
    }

    @Override
    public boolean applyDeleteFilters(List<ResolvedExpression> filters) {
        return storeAs(SupportsDeletePushDown.class).applyDeleteFilters(filters);
    }

    @Override
    public Optional<Long> executeDeletion() {
        return storeAs(SupportsDeletePushDown.class).executeDeletion();
    }

    @Override
    public void executeTruncation() {
        throw new ValidationException(
                String.format(
                        "Cannot TRUNCATE lake-stream table '%s': it would clear the Paimon table "
                                + "and leave the Lakestream Store's copy of the same data behind. "
                                + "Truncate the store's table as well, or set '%s' to false first.",
                        tableIdentifier.asSummaryString(), CoreOptions.LAKE_STREAM_ENABLED.key()));
    }
}
