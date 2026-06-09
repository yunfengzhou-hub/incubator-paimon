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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.StartupMode;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.StreamDataTableScan;

import java.util.Map;

/**
 * Chain-table-aware extension of {@link FallbackReadFileStoreTable}. Inherits the batch read
 * behavior (partition-level fallback between the current branch and {@link ChainGroupReadTable}),
 * and additionally overrides {@link #newStreamScan()} to return a chain-aware {@link
 * ChainTableStreamScan} that performs a partition-level full load followed by incremental
 * delta-only streaming.
 */
public class ChainTableFileStoreTable extends FallbackReadFileStoreTable {

    public ChainTableFileStoreTable(FileStoreTable wrapped, FileStoreTable other) {
        super(wrapped, other, true);
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        CoreOptions coreOptions = wrapped.coreOptions();

        StartupMode effectiveMode = coreOptions.startupMode();
        boolean hasConsumerProgress =
                coreOptions.consumerId() != null && !coreOptions.consumerIgnoreProgress();
        if (effectiveMode != StartupMode.LATEST_FULL || hasConsumerProgress) {
            String reason =
                    describeUnsupportedMode(coreOptions, effectiveMode, hasConsumerProgress);
            throw new UnsupportedOperationException(
                    "Chain table streaming read does not support startup mode '"
                            + reason
                            + "'. "
                            + "Chain table streaming only supports the default 'latest-full' mode, which first "
                            + "produces a partition-level full result and then continuously reads incremental "
                            + "data from the delta branch.\n"
                            + "Suggestions:\n"
                            + "  - To use chain table streaming: remove the explicit scan mode/position settings "
                            + "so that the default 'latest-full' mode is used.\n"
                            + "  - To use standard streaming read without chain table logic: read from a "
                            + "specific branch table (e.g., 't$branch_delta') instead of the main table.");
        }

        // Inherited other() returns the ChainGroupReadTable directly.
        ChainGroupReadTable chainGroupReadTable = (ChainGroupReadTable) other();

        return new ChainTableStreamScan(chainGroupReadTable);
    }

    private static String describeUnsupportedMode(
            CoreOptions coreOptions, StartupMode effectiveMode, boolean hasConsumerProgress) {
        if (hasConsumerProgress) {
            return "consumer-id with existing progress";
        }
        switch (effectiveMode) {
            case LATEST:
                return "scan.mode=latest";
            case FROM_SNAPSHOT:
                if (coreOptions.scanSnapshotId() != null) {
                    return "scan.snapshot-id=" + coreOptions.scanSnapshotId();
                }
                if (coreOptions.scanTagName() != null) {
                    return "scan.tag-name=" + coreOptions.scanTagName();
                }
                if (coreOptions.scanWatermark() != null) {
                    return "scan.watermark=" + coreOptions.scanWatermark();
                }
                return "from-snapshot";
            case FROM_TIMESTAMP:
                if (coreOptions.scanTimestampMills() != null) {
                    return "scan.timestamp-millis=" + coreOptions.scanTimestampMills();
                }
                if (coreOptions.scanTimestamp() != null) {
                    return "scan.timestamp=" + coreOptions.scanTimestamp();
                }
                return "from-timestamp";
            default:
                return effectiveMode.name().toLowerCase().replace('_', '-');
        }
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new ChainTableFileStoreTable(
                wrapped.copy(dynamicOptions), other().copy(rewriteOtherOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new ChainTableFileStoreTable(
                wrapped.copy(newTableSchema),
                other().copy(newTableSchema.copy(rewriteOtherOptions(newTableSchema.options()))));
    }

    @Override
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new ChainTableFileStoreTable(
                wrapped.copyWithoutTimeTravel(dynamicOptions),
                other().copyWithoutTimeTravel(rewriteOtherOptions(dynamicOptions)));
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return new ChainTableFileStoreTable(
                wrapped.copyWithLatestSchema(), other().copyWithLatestSchema());
    }

    @Override
    public FileStoreTable switchToBranch(String branchName) {
        return new ChainTableFileStoreTable(switchWrappedToBranch(branchName), other());
    }
}
