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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.AbstractFlinkTableFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.Table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

/**
 * Decides whether a planned read or write belongs to the Lakestream Store, and delegates it there
 * (PIP-39 L252-350).
 *
 * <p>{@link AbstractFlinkTableFactory} owns the two entry points the PIP names, and asks this class
 * about each of them. Keeping the routing here means the mainline factory only grows a null check
 * and a delegation, so the behaviour of every table that is not a lake-stream table is untouched.
 *
 * <p>One instance belongs to one {@link LakeStreamFlinkCatalog}, which caches the store catalog
 * shared by reads and writes.
 */
public class LakeStreamTableFactoryRouting {

    private final LakeStreamFlinkCatalog lakeStreamCatalog;

    public LakeStreamTableFactoryRouting(LakeStreamFlinkCatalog lakeStreamCatalog) {
        this.lakeStreamCatalog = lakeStreamCatalog;
    }

    /**
     * Returns the store's source for a {@code $rt} read of a lake-stream table, or {@code null}
     * when the read should stay on the Paimon path.
     */
    @Nullable
    public DynamicTableSource routeSource(DynamicTableFactory.Context context) {
        Table baseTable = realTimeReadBaseTable(context);
        if (baseTable == null) {
            return null;
        }
        ObjectIdentifier baseIdentifier =
                LakeStreamFlinkCatalog.stripRealTimeSuffix(context.getObjectIdentifier());
        checkNotFormatTable(baseTable, baseIdentifier);
        org.apache.flink.table.catalog.Catalog storeCatalog =
                lakeStreamCatalog.getOrCreateLakeStreamStoreCatalog(context);
        // for a '$rt' read the catalog already resolved the table from the store, so the planner's
        // table is the store's table with any query hints applied on top of it
        CatalogBaseTable storeTable = context.getCatalogTable();
        DynamicTableSourceFactory sourceFactory =
                storeFactory(
                        storeCatalog,
                        DynamicTableSourceFactory.class,
                        "create a source for '$rt'-suffixed reads");
        return sourceFactory.createDynamicTableSource(
                delegatedContext(context, baseIdentifier, storeTable));
    }

    /**
     * Returns the store's sink for a write to a lake-stream table, or {@code null} when the write
     * should stay on the Paimon path.
     */
    @Nullable
    public DynamicTableSink routeSink(DynamicTableFactory.Context context, Table table) {
        if (persistedLakeStreamTable(identifierOf(context)) == null) {
            return null;
        }
        checkNotFormatTable(table, context.getObjectIdentifier());
        return storeSink(context);
    }

    private DynamicTableSink storeSink(DynamicTableFactory.Context context) {
        ObjectIdentifier identifier = context.getObjectIdentifier();
        org.apache.flink.table.catalog.Catalog storeCatalog =
                lakeStreamCatalog.getOrCreateLakeStreamStoreCatalog(context);
        // the store table is fetched from the store catalog because the store's sink factory may
        // require provider-specific table options (PIP-39 L289)
        CatalogBaseTable storeTable = storeTable(storeCatalog, identifier);
        DynamicTableSinkFactory sinkFactory =
                storeFactory(
                        storeCatalog, DynamicTableSinkFactory.class, "create a sink for writes");
        return sinkFactory.createDynamicTableSink(
                delegatedContext(context, identifier, storeTable));
    }

    /**
     * A lake-stream table has to be a Paimon table: the store holds its recent writes and Paimon
     * holds its history, and a format table has no history to hold. Checked on both paths so that a
     * format table carrying the option cannot end up half routed.
     */
    private void checkNotFormatTable(Table table, ObjectIdentifier identifier) {
        if (table instanceof FormatTable) {
            throw new ValidationException(
                    String.format(
                            "Table '%s' cannot be a lake-stream table: '%s' is only supported on "
                                    + "Paimon tables, not on format tables.",
                            identifier.asSummaryString(), CoreOptions.LAKE_STREAM_ENABLED.key()));
        }
    }

    /**
     * The base Paimon table of a {@code $rt} read that has lake-stream mode enabled, or {@code
     * null} when the read should stay on the Paimon path. {@code $rt} is detected through {@link
     * Identifier} parsing, so branch access like {@code t$branch_b1$rt} is never misrouted.
     */
    @Nullable
    private Table realTimeReadBaseTable(DynamicTableFactory.Context context) {
        Identifier identifier = identifierOf(context);
        if (!LakeStreamFlinkCatalog.isRealTimeTable(identifier)) {
            return null;
        }
        return persistedLakeStreamTable(
                Identifier.create(identifier.getDatabaseName(), identifier.getTableName()));
    }

    /**
     * The persisted Paimon table when lake-stream mode is enabled on it, otherwise {@code null}.
     *
     * <p>Both routing paths read the flag from the catalog rather than from the planner-resolved
     * table, because Flink merges query hints into the latter. A per-statement hint must not be
     * able to move a table's writes between Paimon and the store, which is where the table's recent
     * data lives, and on a {@code $rt} read the planner-resolved table is the store's anyway and
     * carries no such flag.
     */
    @Nullable
    private Table persistedLakeStreamTable(Identifier identifier) {
        try {
            Table table = lakeStreamCatalog.catalog().getTable(identifier);
            return Options.fromMap(table.options()).get(CoreOptions.LAKE_STREAM_ENABLED)
                    ? table
                    : null;
        } catch (Catalog.TableNotExistException e) {
            return null;
        }
    }

    private static Identifier identifierOf(DynamicTableFactory.Context context) {
        ObjectIdentifier identifier = context.getObjectIdentifier();
        return new Identifier(identifier.getDatabaseName(), identifier.getObjectName());
    }

    private CatalogBaseTable storeTable(
            org.apache.flink.table.catalog.Catalog storeCatalog, ObjectIdentifier identifier) {
        return LakeStreamFlinkCatalog.requireStoreTable(
                storeCatalog, identifier.toObjectPath(), configuredStoreIdentifier());
    }

    /**
     * Rejects a write through the {@code $rt} alias, which is read-only: it names the store's view
     * of a lake-stream table, while a write has to name the table itself. Without this the alias
     * reaches the Paimon table lookup and fails as an unknown table.
     */
    public void checkNotRealTimeWrite(DynamicTableFactory.Context context) {
        ObjectIdentifier identifier = context.getObjectIdentifier();
        if (LakeStreamFlinkCatalog.isRealTimeTable(
                new Identifier(identifier.getDatabaseName(), identifier.getObjectName()))) {
            throw new ValidationException(
                    String.format(
                            "Cannot write to '%s': the '$%s' suffix is a read-only alias for the "
                                    + "Lakestream Store's view of a lake-stream table. Write to "
                                    + "'%s' instead.",
                            identifier.asSummaryString(),
                            LakeStreamFlinkCatalog.REAL_TIME_TABLE_SUFFIX,
                            LakeStreamFlinkCatalog.stripRealTimeSuffix(identifier)
                                    .asSummaryString()));
        }
    }

    /**
     * Validates the store catalog factory type before delegation instead of relying on a raw cast
     * (PIP-39 L318), producing a validation error naming the configured store.
     */
    private <T> T storeFactory(
            org.apache.flink.table.catalog.Catalog storeCatalog,
            Class<T> factoryClass,
            String purpose) {
        String store = configuredStoreIdentifier();
        Optional<Factory> factory = storeCatalog.getFactory();
        if (!factory.isPresent() || !factoryClass.isInstance(factory.get())) {
            throw new ValidationException(
                    String.format(
                            "The Lakestream Store '%s' (configured via '%s' = '%s') does not "
                                    + "provide a %s through its catalog factory, so Paimon cannot "
                                    + "delegate lake-stream operations to it and cannot %s.",
                            store,
                            CatalogOptions.LAKE_STREAM_STORE.key(),
                            store,
                            factoryClass.getSimpleName(),
                            purpose));
        }
        return factoryClass.cast(factory.get());
    }

    private String configuredStoreIdentifier() {
        return LakeStreamFlinkCatalog.configuredStoreIdentifier(
                lakeStreamCatalog.catalog().options());
    }

    /**
     * Adapts the planner's context for delegation to the store's table factory (PIP-39 L281): the
     * identifier without the {@code $rt} suffix, the schema resolved for the table, the store
     * table's options, and the provider-specific store catalog options after prefix removal.
     */
    private DynamicTableFactory.Context delegatedContext(
            DynamicTableFactory.Context original,
            ObjectIdentifier identifier,
            CatalogBaseTable storeTable) {
        if (!(storeTable instanceof CatalogTable)) {
            throw new ValidationException(
                    String.format(
                            "Expected a table from the Lakestream Store '%s' for '%s', but got %s.",
                            configuredStoreIdentifier(),
                            identifier.asSummaryString(),
                            storeTable.getClass().getName()));
        }
        ResolvedCatalogTable resolvedStoreTable =
                new ResolvedCatalogTable(
                        (CatalogTable) storeTable, original.getCatalogTable().getResolvedSchema());

        Configuration configuration = new Configuration();
        ReadableConfig originalConfiguration = original.getConfiguration();
        if (originalConfiguration instanceof Configuration) {
            configuration.addAll((Configuration) originalConfiguration);
        } else {
            for (Map.Entry<String, String> entry :
                    AbstractFlinkTableFactory.getAllOptions(original).entrySet()) {
                configuration.setString(entry.getKey(), entry.getValue());
            }
        }
        Map<String, String> storeConnectionOptions =
                LakeStreamFlinkCatalog.extractPrefixedOptions(
                        lakeStreamCatalog.catalog().options(), configuredStoreIdentifier() + ".");
        for (Map.Entry<String, String> entry : storeConnectionOptions.entrySet()) {
            configuration.setString(entry.getKey(), entry.getValue());
        }

        return new DynamicTableFactory.Context() {
            @Override
            public ObjectIdentifier getObjectIdentifier() {
                return identifier;
            }

            @Override
            public ResolvedCatalogTable getCatalogTable() {
                return resolvedStoreTable;
            }

            @Override
            public boolean isTemporary() {
                return false;
            }

            @Override
            public ReadableConfig getConfiguration() {
                return configuration;
            }

            @Override
            public ClassLoader getClassLoader() {
                return original.getClassLoader();
            }
        };
    }
}
