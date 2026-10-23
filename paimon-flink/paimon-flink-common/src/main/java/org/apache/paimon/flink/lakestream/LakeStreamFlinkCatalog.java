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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkTableFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.ExceptionUtils;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The Paimon {@link FlinkCatalog} with lake-stream mode (PIP-39) turned on for a catalog.
 *
 * <p>This extends {@code FlinkCatalog} rather than wrapping one, so that a catalog created for a
 * lake-stream warehouse still is a Paimon Flink catalog: everything the planner and Paimon's own
 * code discover about it by type stays discoverable, and callers that obtain a catalog through
 * Flink's catalog-factory SPI and need a {@code FlinkCatalog} keep receiving one.
 *
 * <p>Ordinary catalog operations are inherited unchanged. Reads of {@code $rt}-suffixed tables are
 * intercepted: the base Paimon table is resolved, {@code lakestream.enabled} is verified, and the
 * store's table for the base identifier is returned so that the store serves the read.
 *
 * <p>This class also owns the Lakestream Store catalog's lifecycle. It discovers, creates, opens
 * and caches that catalog on first use, and closes it in {@link #close()}, which is why {@code
 * FlinkCatalog#close()} is overridable.
 */
public class LakeStreamFlinkCatalog extends FlinkCatalog {

    /** The read-time alias suffix for lake-stream tables, e.g. {@code orders$rt}. */
    public static final String REAL_TIME_TABLE_SUFFIX = "rt";

    /**
     * The Flink configuration of the {@code CREATE CATALOG} statement. {@link
     * #getTable(ObjectPath)} has no table-factory context to draw on, and the planner normally
     * resolves table metadata before it creates a source, so the store catalog is usually first
     * built through that path. Without this, the cached store catalog would permanently miss
     * configuration-carried settings such as platform-injected credential paths.
     */
    private final ReadableConfig catalogConfiguration;

    @Nullable private volatile Catalog lakeStreamStoreCatalog;

    public LakeStreamFlinkCatalog(
            org.apache.paimon.catalog.Catalog catalog,
            String name,
            String defaultDatabase,
            Options options,
            ReadableConfig catalogConfiguration) {
        super(catalog, name, defaultDatabase, options);
        this.catalogConfiguration = catalogConfiguration;
    }

    // ------------------------------------------------------------------------
    //  lifecycle
    // ------------------------------------------------------------------------

    /**
     * Synchronized against {@link #getOrCreateLakeStreamStoreCatalog(ClassLoader, ReadableConfig)},
     * so that a concurrent {@code $rt} resolution cannot cache a store catalog after this method
     * has already taken the field and left it unclosed.
     */
    @Override
    public synchronized void close() throws CatalogException {
        Throwable failure = null;
        try {
            super.close();
        } catch (Throwable t) {
            failure = t;
        }
        Catalog storeCatalog = lakeStreamStoreCatalog;
        lakeStreamStoreCatalog = null;
        if (storeCatalog != null) {
            try {
                storeCatalog.close();
            } catch (Throwable t) {
                failure = ExceptionUtils.firstOrSuppressed(t, failure);
            }
        }
        if (failure != null) {
            ExceptionUtils.rethrow(failure);
        }
    }

    // ------------------------------------------------------------------------
    //  lake-stream store catalog discovery (PIP-39 L209-250)
    // ------------------------------------------------------------------------

    /**
     * The cached Lakestream Store catalog, discovering, creating and opening it on first use from
     * the {@code lakestream.store} catalog option plus the catalog options prefixed with that
     * identifier and a dot, with the prefix removed.
     */
    public Catalog getOrCreateLakeStreamStoreCatalog(DynamicTableFactory.Context context) {
        return getOrCreateLakeStreamStoreCatalog(
                context.getClassLoader(), context.getConfiguration());
    }

    private synchronized Catalog getOrCreateLakeStreamStoreCatalog(
            ClassLoader classLoader, ReadableConfig configuration) {
        Catalog cached = lakeStreamStoreCatalog;
        if (cached != null) {
            return cached;
        }

        Map<String, String> catalogOptions = catalog().options();
        String identifier = catalogOptions.get(CatalogOptions.LAKE_STREAM_STORE.key());
        if (identifier == null || identifier.trim().isEmpty()) {
            throw new CatalogException(
                    String.format(
                            "Lake-stream mode requires the catalog option '%s' to name the "
                                    + "Lakestream Store implementation (e.g. '%s' = 'fluss'), but "
                                    + "catalog '%s' does not set it to a store identifier.",
                            CatalogOptions.LAKE_STREAM_STORE.key(),
                            CatalogOptions.LAKE_STREAM_STORE.key(),
                            getName()));
        }

        CatalogFactory catalogFactory;
        try {
            catalogFactory =
                    FactoryUtil.discoverFactory(
                            classLoader, CatalogFactory.class, identifier.trim());
        } catch (ValidationException e) {
            throw new CatalogException(
                    String.format(
                            "Could not discover a Flink CatalogFactory for the Lakestream Store "
                                    + "'%s' configured via '%s'. Please make sure the store "
                                    + "connector jar is on the classpath.",
                            identifier, CatalogOptions.LAKE_STREAM_STORE.key()),
                    e);
        }

        // per PIP-39: pass every catalog option whose key starts with '<identifier>.'
        // after stripping that prefix, e.g. 'fluss.bootstrap.servers' -> 'bootstrap.servers'
        Map<String, String> storeOptions =
                extractPrefixedOptions(catalogOptions, identifier.trim() + ".");
        CatalogFactory.Context storeContext =
                new FactoryUtil.DefaultCatalogContext(
                        getName(), storeOptions, configuration, classLoader);

        Catalog storeCatalog = catalogFactory.createCatalog(storeContext);
        try {
            storeCatalog.open();
        } catch (Throwable t) {
            try {
                storeCatalog.close();
            } catch (Throwable suppressed) {
                t.addSuppressed(suppressed);
            }
            ExceptionUtils.rethrow(t);
        }
        lakeStreamStoreCatalog = storeCatalog;
        return storeCatalog;
    }

    /**
     * Extracts every entry whose key starts with {@code prefix}, with the prefix removed, e.g.
     * {@code fluss.bootstrap.servers} becomes {@code bootstrap.servers} for the prefix {@code
     * fluss.} (PIP-39 L212).
     */
    public static Map<String, String> extractPrefixedOptions(
            Map<String, String> options, String prefix) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                result.put(entry.getKey().substring(prefix.length()), entry.getValue());
            }
        }
        return result;
    }

    // ------------------------------------------------------------------------
    //  $rt interception
    // ------------------------------------------------------------------------

    /**
     * Whether the parsed Paimon {@link Identifier} is a lake-stream real-time alias, i.e. it has
     * the {@code $rt} system-table suffix and no branch. Branch access such as {@code
     * t1$branch_b1$rt} is deliberately NOT a real-time read.
     */
    public static boolean isRealTimeTable(Identifier identifier) {
        return identifier.getBranchName() == null
                && REAL_TIME_TABLE_SUFFIX.equals(identifier.getSystemTableName());
    }

    /** Strips the {@code $rt} suffix from the object name, keeping catalog and database. */
    public static ObjectIdentifier stripRealTimeSuffix(ObjectIdentifier identifier) {
        Identifier parsed =
                new Identifier(identifier.getDatabaseName(), identifier.getObjectName());
        return ObjectIdentifier.of(
                identifier.getCatalogName(), identifier.getDatabaseName(), parsed.getTableName());
    }

    private static boolean isRealTimeTable(ObjectPath tablePath) {
        return isRealTimeTable(FlinkCatalog.toIdentifier(tablePath));
    }

    private static ObjectPath stripRealTimeSuffix(ObjectPath tablePath) {
        return new ObjectPath(
                tablePath.getDatabaseName(), FlinkCatalog.toIdentifier(tablePath).getTableName());
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!isRealTimeTable(tablePath)) {
            return super.getTable(tablePath);
        }

        ObjectPath basePath = stripRealTimeSuffix(tablePath);
        CatalogBaseTable baseTable = super.getTable(basePath);
        if (!Options.fromMap(baseTable.getOptions()).get(CoreOptions.LAKE_STREAM_ENABLED)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        Catalog storeCatalog =
                getOrCreateLakeStreamStoreCatalog(
                        Thread.currentThread().getContextClassLoader(), catalogConfiguration);
        return requireStoreTable(storeCatalog, basePath, storeIdentifier());
    }

    /**
     * The store's view of a lake-stream table. The store side is created by the store's own
     * enablement flow rather than by Paimon, so it can be missing, and the store's own not-exists
     * error would name the base table and this catalog rather than explain that the store side is
     * what is absent.
     */
    static CatalogBaseTable requireStoreTable(
            Catalog storeCatalog, ObjectPath tablePath, String storeIdentifier) {
        try {
            return storeCatalog.getTable(tablePath);
        } catch (TableNotExistException e) {
            throw new ValidationException(
                    String.format(
                            "Table '%s' does not exist in the Lakestream Store '%s'. Lake-stream "
                                    + "mode requires the corresponding store-side table to exist "
                                    + "for Paimon table '%s'.",
                            tablePath.getFullName(), storeIdentifier, tablePath.getFullName()),
                    e);
        }
    }

    /**
     * The configured store identifier for error messages, or {@code <unset>} when there is none.
     */
    static String configuredStoreIdentifier(Map<String, String> catalogOptions) {
        String store = catalogOptions.get(CatalogOptions.LAKE_STREAM_STORE.key());
        return store == null || store.trim().isEmpty() ? "<unset>" : store.trim();
    }

    private String storeIdentifier() {
        return configuredStoreIdentifier(catalog().options());
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.17-.
     */
    public CatalogBaseTable getTable(ObjectPath tablePath, long timestamp)
            throws TableNotExistException, CatalogException {
        if (isRealTimeTable(tablePath)) {
            throw new CatalogException(
                    String.format(
                            "Lake-stream real-time table '%s' does not support time travel reads. "
                                    + "The '$%s' suffix already provides the freshest data by "
                                    + "combining Paimon and Lakestream Store data; use time travel "
                                    + "on the original table '%s' instead.",
                            tablePath.getFullName(),
                            REAL_TIME_TABLE_SUFFIX,
                            stripRealTimeSuffix(tablePath).getFullName()));
        }
        return super.getTable(tablePath, timestamp);
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new FlinkTableFactory(this, new LakeStreamTableFactoryRouting(this)));
    }
}
