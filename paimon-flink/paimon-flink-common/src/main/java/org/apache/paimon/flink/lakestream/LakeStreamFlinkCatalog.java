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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.ProcedureNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.procedures.Procedure;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A Flink catalog wrapper around the Paimon {@link FlinkCatalog} that adds lake-stream mode
 * (PIP-39) support.
 *
 * <p>Composition is used instead of inheritance because {@link FlinkCatalog#open()} and {@link
 * FlinkCatalog#close()} are {@code final}, while the wrapper must hook {@code close()} to close the
 * cached lake-stream store catalog it created (PIP-39: "The Paimon catalog wrapper closes the
 * cached Lakestream Store catalog when it is closed"). {@link
 * org.apache.paimon.flink.FlinkGenericCatalog} is the in-repo precedent for this pattern.
 *
 * <p>All ordinary catalog operations are delegated to the wrapped Paimon catalog unchanged. Only
 * reads of {@code $rt}-suffixed tables are intercepted: the base Paimon table is resolved, {@code
 * lakestream.enabled} is verified, and the store catalog's table for the base identifier is
 * returned so the store can serve the union read.
 */
public class LakeStreamFlinkCatalog extends AbstractCatalog
        implements LakeStreamStoreCatalogProvider {

    /** The read-time alias suffix for lake-stream tables, e.g. {@code orders$rt}. */
    public static final String REAL_TIME_TABLE_SUFFIX = "rt";

    private final FlinkCatalog paimon;

    /**
     * The Flink configuration of the {@code CREATE CATALOG} statement. {@link
     * #getTable(ObjectPath)} has no table-factory context to draw on, and the planner normally
     * resolves table metadata before it creates a source, so the store catalog is usually first
     * built through that path. Without this, the cached store catalog would permanently miss
     * configuration-carried settings such as platform-injected credential paths.
     */
    private final ReadableConfig catalogConfiguration;

    @Nullable private volatile Catalog lakeStreamStoreCatalog;

    public LakeStreamFlinkCatalog(FlinkCatalog paimon) {
        this(paimon, new Configuration());
    }

    public LakeStreamFlinkCatalog(FlinkCatalog paimon, ReadableConfig catalogConfiguration) {
        super(paimon.getName(), paimon.getDefaultDatabase());
        this.paimon = paimon;
        this.catalogConfiguration = catalogConfiguration;
    }

    /** Returns the wrapped Paimon Flink catalog. */
    public FlinkCatalog paimonCatalog() {
        return paimon;
    }

    /**
     * Unwraps the Paimon {@link FlinkCatalog} from a catalog obtained through {@link
     * org.apache.paimon.flink.FlinkCatalogFactory}. Callers that discover that factory through
     * Flink's SPI receive Flink's {@link Catalog}, because a lake-stream catalog wraps the Paimon
     * catalog instead of extending it.
     */
    public static FlinkCatalog unwrapPaimonCatalog(Catalog catalog) {
        if (catalog instanceof LakeStreamFlinkCatalog) {
            return ((LakeStreamFlinkCatalog) catalog).paimonCatalog();
        }
        return (FlinkCatalog) catalog;
    }

    // ------------------------------------------------------------------------
    //  lifecycle
    // ------------------------------------------------------------------------

    @Override
    public void open() throws CatalogException {
        paimon.open();
    }

    /**
     * Synchronized against {@link #getOrCreateLakeStreamStoreCatalog(ClassLoader, ReadableConfig)},
     * so that a concurrent {@code $rt} resolution cannot cache a store catalog after this method
     * has already taken the field and left it unclosed.
     */
    @Override
    public synchronized void close() throws CatalogException {
        Throwable failure = null;
        try {
            paimon.close();
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

    @Override
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

        Map<String, String> catalogOptions = paimon.catalog().options();
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
            return paimon.getTable(tablePath);
        }

        ObjectPath basePath = stripRealTimeSuffix(tablePath);
        CatalogBaseTable baseTable = paimon.getTable(basePath);
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
        return configuredStoreIdentifier(paimon.catalog().options());
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
        return paimon.getTable(tablePath, timestamp);
    }

    // ------------------------------------------------------------------------
    //  delegation to the wrapped Paimon catalog
    // ------------------------------------------------------------------------

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(
                new FlinkTableFactory(paimon, new LakeStreamTableFactoryRouting(paimon, this)));
    }

    @Override
    public Optional<FunctionDefinitionFactory> getFunctionDefinitionFactory() {
        return paimon.getFunctionDefinitionFactory();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return paimon.listDatabases();
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return paimon.getDatabase(databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return paimon.databaseExists(databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        paimon.createDatabase(name, database, ignoreIfExists);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        paimon.dropDatabase(name, ignoreIfNotExists, cascade);
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        paimon.alterDatabase(name, newDatabase, ignoreIfNotExists);
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return paimon.listTables(databaseName);
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return paimon.listViews(databaseName);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        return paimon.tableExists(tablePath);
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        paimon.dropTable(tablePath, ignoreIfNotExists);
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        paimon.renameTable(tablePath, newTableName, ignoreIfNotExists);
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        paimon.createTable(tablePath, table, ignoreIfExists);
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        paimon.alterTable(tablePath, newTable, ignoreIfNotExists);
    }

    @Override
    public void alterTable(
            ObjectPath tablePath,
            CatalogBaseTable newTable,
            List<TableChange> tableChanges,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        paimon.alterTable(tablePath, newTable, tableChanges, ignoreIfNotExists);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return paimon.listPartitions(tablePath);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, CatalogException {
        return paimon.listPartitions(tablePath, partitionSpec);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return paimon.listPartitionsByFilter(tablePath, filters);
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return paimon.getPartition(tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return paimon.partitionExists(tablePath, partitionSpec);
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, PartitionAlreadyExistsException,
                    CatalogException {
        paimon.createPartition(tablePath, partitionSpec, partition, ignoreIfExists);
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        paimon.dropPartition(tablePath, partitionSpec, ignoreIfNotExists);
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        paimon.alterPartition(tablePath, partitionSpec, newPartition, ignoreIfNotExists);
    }

    @Override
    public List<String> listFunctions(String dbName)
            throws DatabaseNotExistException, CatalogException {
        return paimon.listFunctions(dbName);
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        return paimon.getFunction(functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return paimon.functionExists(functionPath);
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        paimon.createFunction(functionPath, function, ignoreIfExists);
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        paimon.alterFunction(functionPath, newFunction, ignoreIfNotExists);
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        paimon.dropFunction(functionPath, ignoreIfNotExists);
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return paimon.getTableStatistics(tablePath);
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return paimon.getTableColumnStatistics(tablePath);
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return paimon.getPartitionStatistics(tablePath, partitionSpec);
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return paimon.getPartitionColumnStatistics(tablePath, partitionSpec);
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        paimon.alterTableStatistics(tablePath, tableStatistics, ignoreIfNotExists);
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {
        paimon.alterTableColumnStatistics(tablePath, columnStatistics, ignoreIfNotExists);
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        paimon.alterPartitionStatistics(
                tablePath, partitionSpec, partitionStatistics, ignoreIfNotExists);
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        paimon.alterPartitionColumnStatistics(
                tablePath, partitionSpec, columnStatistics, ignoreIfNotExists);
    }

    @Override
    public List<CatalogTableStatistics> bulkGetPartitionStatistics(
            ObjectPath tablePath, List<CatalogPartitionSpec> partitionSpecs)
            throws PartitionNotExistException, CatalogException {
        return paimon.bulkGetPartitionStatistics(tablePath, partitionSpecs);
    }

    @Override
    public List<CatalogColumnStatistics> bulkGetPartitionColumnStatistics(
            ObjectPath tablePath, List<CatalogPartitionSpec> partitionSpecs)
            throws PartitionNotExistException, CatalogException {
        return paimon.bulkGetPartitionColumnStatistics(tablePath, partitionSpecs);
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.17-.
     */
    public List<String> listProcedures(String dbName)
            throws DatabaseNotExistException, CatalogException {
        return paimon.listProcedures(dbName);
    }

    /**
     * Do not annotate with <code>@override</code> here to maintain compatibility with Flink 1.17-.
     */
    public Procedure getProcedure(ObjectPath procedurePath)
            throws ProcedureNotExistException, CatalogException {
        return paimon.getProcedure(procedurePath);
    }
}
