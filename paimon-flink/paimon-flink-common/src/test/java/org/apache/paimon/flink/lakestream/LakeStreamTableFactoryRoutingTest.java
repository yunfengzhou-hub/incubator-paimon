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
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkTableFactory;
import org.apache.paimon.flink.PaimonDataStreamSinkProvider;
import org.apache.paimon.flink.sink.FlinkTableSink;
import org.apache.paimon.flink.source.DataTableSource;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FormatTable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.connector.sink.abilities.SupportsTruncate;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Tests for the lake-stream routing in {@link org.apache.paimon.flink.AbstractFlinkTableFactory}
 * (PIP-39 source creation L252-281 and sink creation L283-350), exercised through {@link
 * FlinkTableFactory} created by {@link LakeStreamFlinkCatalog#getFactory()} with the fake store.
 */
public class LakeStreamTableFactoryRoutingTest {

    private static final ObjectPath T1 = new ObjectPath("db1", "t1");
    private static final ObjectPath T1_RT = new ObjectPath("db1", "t1$rt");
    private static final ObjectPath T2 = new ObjectPath("db1", "t2");

    @TempDir java.nio.file.Path tempDir;

    @BeforeEach
    public void beforeEach() {
        FakeStoreCatalogFactory.reset();
    }

    // ------------------------------------------------------------------------
    //  helpers
    // ------------------------------------------------------------------------

    private Options conf(String factoryMode) {
        Options conf = new Options();
        conf.setString(CatalogOptions.WAREHOUSE.key(), tempDir.toString());
        conf.setString(CatalogOptions.LAKE_STREAM_STORE.key(), FakeStoreCatalogFactory.IDENTIFIER);
        conf.setString("fakestore.bootstrap.servers", "localhost:9123");
        if (factoryMode != null) {
            conf.setString("fakestore." + FakeStoreCatalog.FACTORY_MODE_OPTION, factoryMode);
        }
        return conf;
    }

    private LakeStreamFlinkCatalog newCatalog(String factoryMode) {
        org.apache.flink.table.catalog.Catalog catalog =
                new FlinkCatalogFactory()
                        .createCatalog(
                                new FactoryUtil.DefaultCatalogContext(
                                        "test-catalog",
                                        conf(factoryMode).toMap(),
                                        new Configuration(),
                                        getClass().getClassLoader()));
        return (LakeStreamFlinkCatalog) catalog;
    }

    private static ResolvedSchema idSchema() {
        return new ResolvedSchema(
                Collections.singletonList(Column.physical("id", DataTypes.INT().notNull())),
                Collections.<WatermarkSpec>emptyList(),
                UniqueConstraint.primaryKey("PK_id", Collections.singletonList("id")));
    }

    private void createTable(
            LakeStreamFlinkCatalog catalog, ObjectPath path, boolean lakeStreamEnabled)
            throws Exception {
        catalog.createDatabase(
                path.getDatabaseName(), new CatalogDatabaseImpl(new HashMap<>(), null), true);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        if (lakeStreamEnabled) {
            options.put(CoreOptions.LAKE_STREAM_ENABLED.key(), "true");
        }
        ResolvedSchema resolvedSchema = idSchema();
        CatalogTable table =
                new ResolvedCatalogTable(
                        CatalogTable.newBuilder()
                                .schema(
                                        Schema.newBuilder()
                                                .fromResolvedSchema(resolvedSchema)
                                                .build())
                                .partitionKeys(Collections.<String>emptyList())
                                .options(options)
                                .build(),
                        resolvedSchema);
        catalog.createTable(path, table, false);
    }

    private static ResolvedCatalogTable resolved(CatalogBaseTable table, ResolvedSchema schema) {
        return new ResolvedCatalogTable((CatalogTable) table, schema);
    }

    private DynamicTableFactory.Context context(
            ObjectIdentifier identifier, ResolvedCatalogTable table) {
        return new DynamicTableFactory.Context() {
            @Override
            public ObjectIdentifier getObjectIdentifier() {
                return identifier;
            }

            @Override
            public ResolvedCatalogTable getCatalogTable() {
                return table;
            }

            @Override
            public boolean isTemporary() {
                return false;
            }

            @Override
            public ReadableConfig getConfiguration() {
                return new Configuration();
            }

            @Override
            public ClassLoader getClassLoader() {
                return getClass().getClassLoader();
            }
        };
    }

    private static ObjectIdentifier id(String objectName) {
        return ObjectIdentifier.of("test-catalog", "db1", objectName);
    }

    private static DynamicTableSink.Context sinkContext(boolean bounded) {
        return new DynamicTableSink.Context() {
            @Override
            public boolean isBounded() {
                return bounded;
            }

            @Override
            public <T> TypeInformation<T> createTypeInformation(DataType dataType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> TypeInformation<T> createTypeInformation(LogicalType logicalType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DynamicTableSink.DataStructureConverter createDataStructureConverter(
                    DataType dataType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<int[][]> getTargetColumns() {
                return Optional.empty();
            }
        };
    }

    // ------------------------------------------------------------------------
    //  source routing (PIP-39 L252-281)
    // ------------------------------------------------------------------------

    @Test
    public void testRealTimeReadRoutesToStoreSourceFactory() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        // the planner resolves 't1$rt' through the wrapper, which returns the store table
        ResolvedCatalogTable storeTable =
                resolved(
                        catalog.getTable(T1_RT),
                        new ResolvedSchema(
                                Collections.singletonList(Column.physical("id", DataTypes.INT())),
                                Collections.<WatermarkSpec>emptyList(),
                                null));
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSource source =
                factory.createDynamicTableSource(context(id("t1$rt"), storeTable));

        assertThat(source).isInstanceOf(FakeStoreSource.class);
        assertThat(FakeStoreTableFactory.sourceContexts()).hasSize(1);
        DynamicTableFactory.Context delegated = FakeStoreTableFactory.sourceContexts().get(0);
        // delegated with the BASE identifier, without the '$rt' suffix
        assertThat(delegated.getObjectIdentifier().getObjectName()).isEqualTo("t1");
        assertThat(delegated.getObjectIdentifier().getDatabaseName()).isEqualTo("db1");
        // the store table's options
        assertThat(delegated.getCatalogTable().getOptions())
                .containsEntry("connector", "fakestore");
        // the store connection options after prefix removal
        assertThat(delegated.getConfiguration().get(FakeStoreCatalogFactory.BOOTSTRAP_SERVERS))
                .isEqualTo("localhost:9123");
        catalog.close();
    }

    @Test
    public void testQueryHintsReachTheStoreSourceFactory() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        // the planner applies '/*+ OPTIONS(...) */' on top of the table the catalog resolved, and a
        // real store needs those hints to reach its source factory, e.g. to bound a log scan
        CatalogBaseTable storeTable = catalog.getTable(T1_RT);
        Map<String, String> hintedOptions = new HashMap<>(storeTable.getOptions());
        hintedOptions.put("scan.startup.mode", "earliest");
        ResolvedCatalogTable hinted =
                resolved(
                        ((CatalogTable) storeTable).copy(hintedOptions),
                        new ResolvedSchema(
                                Collections.singletonList(Column.physical("id", DataTypes.INT())),
                                Collections.<WatermarkSpec>emptyList(),
                                null));
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        factory.createDynamicTableSource(context(id("t1$rt"), hinted));

        DynamicTableFactory.Context delegated = FakeStoreTableFactory.sourceContexts().get(0);
        assertThat(delegated.getCatalogTable().getOptions())
                .containsEntry("connector", "fakestore")
                .containsEntry("scan.startup.mode", "earliest");
        catalog.close();
    }

    @Test
    public void testOriginalNameReadCreatesPaimonSource() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSource source =
                factory.createDynamicTableSource(context(id("t1"), paimonTable));

        assertThat(source).isInstanceOf(DataTableSource.class);
        assertThat(FakeStoreTableFactory.sourceContexts()).isEmpty();
        catalog.close();
    }

    @Test
    public void testRealTimeReadOnNonLakeStreamTableIsNotRouted() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T2, false);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T2), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSource source =
                factory.createDynamicTableSource(context(id("t2$rt"), paimonTable));

        // the wrapper already rejects 't2$rt' resolution; at factory level the gate must not route
        // a '$rt' identifier whose base table has lakestream.enabled = false
        assertThat(source).isNotInstanceOf(FakeStoreSource.class);
        assertThat(FakeStoreTableFactory.sourceContexts()).isEmpty();
        catalog.close();
    }

    @Test
    public void testRealTimeReadWithBranchIsNotRouted() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSource source =
                factory.createDynamicTableSource(context(id("t1$branch_b1$rt"), paimonTable));

        // a branch access must never be misrouted to the store ('endsWith($rt)' would misroute it)
        assertThat(source).isNotInstanceOf(FakeStoreSource.class);
        assertThat(FakeStoreTableFactory.sourceContexts()).isEmpty();
        catalog.close();
    }

    @Test
    public void testRealTimeReadWithoutProviderIsNotRouted() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        // connector mode: no LakeStreamStoreCatalogProvider
        FlinkTableFactory factory = new FlinkTableFactory();

        DynamicTableSource source =
                factory.createDynamicTableSource(context(id("t1$rt"), paimonTable));

        assertThat(source).isNotInstanceOf(FakeStoreSource.class);
        assertThat(FakeStoreTableFactory.sourceContexts()).isEmpty();
        catalog.close();
    }

    @Test
    public void testRealTimeReadFailsWhenStoreExposesNoSourceFactory() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog("sink-only");
        createTable(catalog, T1, true);
        ResolvedCatalogTable storeTable =
                resolved(
                        catalog.getTable(T1_RT),
                        new ResolvedSchema(
                                Collections.singletonList(Column.physical("id", DataTypes.INT())),
                                Collections.<WatermarkSpec>emptyList(),
                                null));
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        assertThatThrownBy(() -> factory.createDynamicTableSource(context(id("t1$rt"), storeTable)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("fakestore");
        catalog.close();
    }

    // ------------------------------------------------------------------------
    //  sink routing (PIP-39 L283-350)
    // ------------------------------------------------------------------------

    @Test
    public void testSinkIsAPaimonSinkThatRoutesByOverwrite() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSink sink = factory.createDynamicTableSink(context(id("t1"), paimonTable));

        // the routed sink is a Paimon sink, so it inherits every ability Paimon's sink has
        assertThat(sink).isInstanceOf(LakeStreamFlinkTableSink.class);
        assertThat(sink).isInstanceOf(FlinkTableSink.class);

        // the store sink is built through the delegated context
        assertThat(FakeStoreTableFactory.sinkContexts()).hasSize(1);
        DynamicTableFactory.Context delegated = FakeStoreTableFactory.sinkContexts().get(0);
        // regular writes keep the ORIGINAL identifier (no '$rt' involved)
        assertThat(delegated.getObjectIdentifier().getObjectName()).isEqualTo("t1");
        assertThat(delegated.getCatalogTable().getOptions())
                .containsEntry("connector", "fakestore");
        assertThat(delegated.getConfiguration().get(FakeStoreCatalogFactory.BOOTSTRAP_SERVERS))
                .isEqualTo("localhost:9123");

        assertThat(sink).isInstanceOf(SupportsOverwrite.class);
        assertThat(sink).isInstanceOf(SupportsPartitioning.class);
        assertThat(sink).isInstanceOf(SupportsTruncate.class);

        // default (regular writes): the store sink serves the runtime provider
        assertThat(sink.asSummaryString()).isEqualTo("FakeStoreSink");
        assertThat(sink.getSinkRuntimeProvider(sinkContext(true)))
                .isInstanceOf(SinkV2Provider.class);

        // INSERT OVERWRITE: the Paimon side serves the runtime provider
        ((SupportsOverwrite) sink).applyOverwrite(true);
        assertThat(sink.asSummaryString()).isNotEqualTo("FakeStoreSink");
        assertThat(sink.getSinkRuntimeProvider(sinkContext(true)))
                .isInstanceOf(PaimonDataStreamSinkProvider.class);
        catalog.close();
    }

    @Test
    public void testRowLevelOperationsAreForwardedToTheStore() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();
        FakeStoreSink.resetAbilityCalls();

        DynamicTableSink sink = factory.createDynamicTableSink(context(id("t1"), paimonTable));

        // a DELETE/UPDATE is a regular write, so the store executes it
        ((SupportsRowLevelDelete) sink).applyRowLevelDelete(null);
        ((SupportsRowLevelUpdate) sink)
                .applyRowLevelUpdate(
                        Collections.singletonList(Column.physical("id", DataTypes.INT())), null);
        ((SupportsDeletePushDown) sink)
                .applyDeleteFilters(Collections.<ResolvedExpression>emptyList());
        ((SupportsDeletePushDown) sink).executeDeletion();

        assertThat(FakeStoreSink.abilityCalls())
                .contains(
                        "applyRowLevelDelete",
                        "applyRowLevelUpdate(1)",
                        "applyDeleteFilters(0)",
                        "executeDeletion");
        catalog.close();
    }

    @Test
    public void testTruncateIsRejectedOnALakeStreamTable() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSink sink = factory.createDynamicTableSink(context(id("t1"), paimonTable));

        // truncating Paimon alone would leave the store's copy of the same data behind
        assertThatThrownBy(() -> ((SupportsTruncate) sink).executeTruncation())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("TRUNCATE")
                .hasMessageContaining(CoreOptions.LAKE_STREAM_ENABLED.key());
        catalog.close();
    }

    @Test
    public void testCopyKeepsTheRouting() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSink sink = factory.createDynamicTableSink(context(id("t1"), paimonTable));
        DynamicTableSink copied = sink.copy();

        // the inherited copy() hardcodes 'new FlinkTableSink', which would drop the routing
        assertThat(copied).isInstanceOf(LakeStreamFlinkTableSink.class);
        assertThat(copied.asSummaryString()).isEqualTo("FakeStoreSink");

        ((SupportsOverwrite) copied).applyOverwrite(true);
        assertThat(copied.getSinkRuntimeProvider(sinkContext(true)))
                .isInstanceOf(PaimonDataStreamSinkProvider.class);
        catalog.close();
    }

    @Test
    public void testStaticPartitionGoesToTheExecutingSide() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();
        FakeStoreSink.resetAbilityCalls();

        DynamicTableSink sink = factory.createDynamicTableSink(context(id("t1"), paimonTable));

        Map<String, String> partition = new HashMap<>();
        partition.put("dt", "20261019");

        // a regular write is executed by the store
        ((SupportsPartitioning) sink).applyStaticPartition(partition);
        assertThat(FakeStoreSink.abilityCalls()).contains("applyStaticPartition({dt=20261019})");

        // an overwrite is executed by Paimon, so the store must not be configured for it, and the
        // flag is not forwarded to it either: a store that validates the flag would otherwise
        // reject a statement it never runs
        FakeStoreSink.resetAbilityCalls();
        ((SupportsOverwrite) sink).applyOverwrite(true);
        ((SupportsPartitioning) sink).applyStaticPartition(partition);
        assertThat(FakeStoreSink.abilityCalls()).isEmpty();
        catalog.close();
    }

    @Test
    public void testFormatTableCannotBeALakeStreamTable() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        LakeStreamTableFactoryRouting routing =
                new LakeStreamTableFactoryRouting(catalog.paimonCatalog(), catalog);

        // a format table has no Paimon history for the store to tier into; the same check guards
        // the '$rt' read path, which needs a catalog that supports format tables to reach
        assertThatThrownBy(
                        () ->
                                routing.routeSink(
                                        context(id("t1"), paimonTable), mock(FormatTable.class)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("format tables")
                .hasMessageContaining(CoreOptions.LAKE_STREAM_ENABLED.key());
        catalog.close();
    }

    @Test
    public void testSinkNotRoutedWhenLakeStreamDisabled() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T2, false);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T2), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSink sink = factory.createDynamicTableSink(context(id("t2"), paimonTable));

        assertThat(sink).isNotInstanceOf(LakeStreamFlinkTableSink.class);
        assertThat(FakeStoreTableFactory.sinkContexts()).isEmpty();
        catalog.close();
    }

    @Test
    public void testSinkNotRoutedWithoutProvider() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        // connector mode: no LakeStreamStoreCatalogProvider
        FlinkTableFactory factory = new FlinkTableFactory();

        DynamicTableSink sink = factory.createDynamicTableSink(context(id("t1"), paimonTable));

        assertThat(sink).isNotInstanceOf(LakeStreamFlinkTableSink.class);
        assertThat(FakeStoreTableFactory.sinkContexts()).isEmpty();
        catalog.close();
    }

    @Test
    public void testSinkFailsWhenStoreExposesNoSinkFactory() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog("source-only");
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        assertThatThrownBy(() -> factory.createDynamicTableSink(context(id("t1"), paimonTable)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("fakestore");
        catalog.close();
    }

    @Test
    public void testSinkFailsClearlyWhenTheStoreSideIsMissing() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        FakeStoreCatalog.markTableMissing(T1);
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());

        assertThatThrownBy(() -> factory.createDynamicTableSink(context(id("t1"), paimonTable)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("does not exist in the Lakestream Store")
                .hasMessageContaining(FakeStoreCatalogFactory.IDENTIFIER);
        catalog.close();
    }

    @Test
    public void testWriteThroughTheRealTimeAliasIsRejected() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();
        ResolvedCatalogTable storeTable = resolved(catalog.getTable(T1_RT), idSchema());

        assertThatThrownBy(() -> factory.createDynamicTableSink(context(id("t1$rt"), storeTable)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("read-only alias")
                .hasMessageContaining("test-catalog.db1.t1");
        catalog.close();
    }

    @Test
    public void testSinkFailsWhenStoreExposesNoFactoryAtAll() throws Exception {
        LakeStreamFlinkCatalog catalog = newCatalog("none");
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        assertThatThrownBy(() -> factory.createDynamicTableSink(context(id("t1"), paimonTable)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("fakestore");
        catalog.close();
    }
}
